package com.sbartnik.layers.speed

import com.sbartnik.config.AppConfig
import com.sbartnik.domain.{ActionBySite, SiteActionRecord}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object SparkStreamingConsumer extends App {

  val conf = AppConfig
  val kafkaConf = conf.Kafka

  val kafkaDirectParams = Map(
    "metadata.broker.list" -> kafkaConf.Producer.bootstrapServers,
    "group.id" -> kafkaConf.StreamConsumer.groupId,
    "auto.offset.reset" -> kafkaConf.StreamConsumer.autoOffsetReset
  )

  val checkpointDirectory = conf.checkpointDir
  val streamingBatchDuration = Seconds(conf.streamingBatchDurationSeconds)
  val dataPath = conf.hdfsDataPath

  val ss = SparkSession
    .builder()
    .appName("Spark Streaming Processing")
    .master(conf.sparkMaster)
    .config("spark.sql.warehouse.dir", "file:///${system:user.dir}/spark-warehouse")
    .getOrCreate()

  import ss.implicits._

  var sc = ss.sparkContext
  sc.setCheckpointDir(checkpointDirectory)
  val sqlc = ss.sqlContext

  def sparkStreamingCreateFunction(): StreamingContext = {
    val ssc = new StreamingContext(sc, streamingBatchDuration)

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaDirectParams, Set(kafkaConf.topic)
    )

    val siteActionRecordStream = kafkaDirectStream
      .transform(SiteActionRecord.fromStringRDDToRDD)
      .cache()

    /////////////////////
    // Save to HDFS
    /////////////////////
    siteActionRecordStream.foreachRDD(rdd => {
      val siteActionRecordDF = rdd
        .toDF()
        .selectExpr(
          "timestamp", "timestampBucket", "referrer", "action",
          "previousPage", "visitor", "geo",
          "timeSpentSeconds", "subPage", "site",
          "props.topic as topic",
          "props.kafkaPartition as kafkaPartition",
          "props.fromOffset as fromOffset",
          "props.untilOffset as untilOffset")

      siteActionRecordDF
        .write
        .partitionBy("topic", "kafkaPartition", "timestampBucket")
        .mode(SaveMode.Append)
        .parquet(dataPath)
    })

    //////////////////////////
    // Compute unique visitors
    //////////////////////////

    //////////////////////////
    // Compute action by site
    //////////////////////////

    val actionsBySiteStateSpec = StateSpec
      .function((k: (String, Long), v: Option[ActionBySite], state: State[(Long, Long, Long)]) => {
        var (favCount, commCount, viewCount) = state.getOption().getOrElse((0L, 0L, 0L))
        val newVal = v match {
          case Some(x) => (x.favCount, x.commCount, x.viewCount)
          case _ => (0L, 0L, 0L)
        }

        favCount  += newVal._1
        commCount += newVal._2
        viewCount += newVal._3

        state.update((favCount, commCount, viewCount))
      })
      .timeout(Minutes(120))

    val actionBySite = siteActionRecordStream.transform(rdd => {
      rdd.toDF().createOrReplaceTempView("records")
      val actionsBySite = sqlc.sql(
        """SELECT site, timestamp_bucket,
            |SUM(CASE WHEN action = 'add_to_favorites' THEN 1 ELSE 0 END) as fav_count,
            |SUM(CASE WHEN action = 'comment' THEN 1 ELSE 0 END) as comm_count,
            |SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) as view_count
          |FROM records
          |GROUP BY site, timestampBucket
          |ORDER BY site
        """.stripMargin
      )

      actionsBySite.map(x => (
        (x.getString(0), x.getLong(1)),
        ActionBySite(x.getString(0), x.getLong(1), x.getLong(2), x.getLong(3), x.getLong(4))
      )).rdd
    })

    val reducedActionBySite = actionBySite
      .mapWithState(actionsBySiteStateSpec)
      .stateSnapshots()
      .reduceByKeyAndWindow(
        (a, b) => b,
        (a, b) => a,
        Seconds(conf.streamingWindowDurationSeconds / conf.streamingBatchDurationSeconds * conf.streamingBatchDurationSeconds)
      )

    val mappedActionBySite = reducedActionBySite.map(x => ActionBySite(x._1._1, x._1._2, x._2._1, x._2._2, x._2._3))
    mappedActionBySite.saveToCassandra(conf.Cassandra.keyspaceName, conf.Cassandra.tables.get(1))
    //mappedActionBySite.print(200)

    ssc
  }

  val ssc = sc.getCheckpointDir match {
    case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, sparkStreamingCreateFunction, sc.hadoopConfiguration, createOnError = true)
    case None => StreamingContext.getActiveOrCreate(sparkStreamingCreateFunction)
  }

  sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
  ssc.start()
  ssc.awaitTermination()
}
