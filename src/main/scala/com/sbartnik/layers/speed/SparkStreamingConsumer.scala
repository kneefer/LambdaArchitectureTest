package com.sbartnik.layers.speed

import com.metamx.common.Granularity
import com.metamx.tranquility.beam.{Beam, ClusteredBeamTuning}
import com.metamx.tranquility.druid.{DruidBeams, DruidLocation, DruidRollup, SpecificDruidDimensions}
import com.metamx.tranquility.spark.BeamFactory
import com.sbartnik.config.AppConfig
import com.sbartnik.domain.{ActionBySite, SiteActionRecord}
import io.druid.granularity.QueryGranularities
import io.druid.query.aggregation.LongSumAggregatorFactory
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.joda.time.{DateTime, Period}

class SiteActionRecordBeamFactory extends BeamFactory[ActionBySite]
{
  // Return a singleton, so the same connection is shared across all tasks in the same JVM.
  def makeBeam: Beam[ActionBySite] = SiteActionRecordBeamFactory.BeamInstance
}

object SiteActionRecordBeamFactory
{
  val BeamInstance: Beam[ActionBySite] = {
    // Tranquility uses ZooKeeper (through Curator framework) for coordination.
    val curator = CuratorFrameworkFactory.newClient(
      "localhost:3002",
      new BoundedExponentialBackoffRetry(100, 3000, 5)
    )
    curator.start()

    val indexService = "druid/overlord" // Your overlord's druid.service, with slashes replaced by colons.
    val firehoseService = ""
    val discoveryPath = "/druid/discovery"     // Your overlord's druid.discovery.curator.path
    val dataSource = "foo"
    val dimensions = IndexedSeq("bar")
    val aggregators = Seq(new LongSumAggregatorFactory("baz", "baz"))

    // Expects simpleEvent.timestamp to return a Joda DateTime object.
    DruidBeams
      .builder((simpleEvent: ActionBySite) => new DateTime(simpleEvent.timestampBucket))
      .curator(curator)
      .discoveryPath(discoveryPath)
      .location(DruidLocation(indexService, firehoseService, dataSource))
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularities.MINUTE))
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = Granularity.HOUR,
          windowPeriod = new Period("PT10M"),
          partitions = 1,
          replicants = 1
        )
      )
      .buildBeam()
  }
}

object SparkStreamingConsumer extends App {

  val conf = AppConfig
  val kafkaConf = conf.Kafka

  val kafkaDirectParams = Map(
    "metadata.broker.list" -> kafkaConf.Producer.bootstrapServers,
    "group.id" -> kafkaConf.HdfsConsumer.groupId,
    "auto.offset.reset" -> kafkaConf.HdfsConsumer.autoOffsetReset
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
    // Compute action by site
    //////////////////////////

    val actionsBySiteStateSpec = StateSpec
      .function((k: (String, Long), v: Option[ActionBySite], state: State[(Long, Long, Long)]) => {
        var (favCount, commCount, viewCount) = state.getOption().getOrElse((0L, 0L, 0L))
        val newVal = v match {
          case Some(x) => (x.favCount, x.commCount, x.viewCount)
          case _ => (0L, 0L, 0L)
        }

        favCount += newVal._1
        commCount += newVal._2
        viewCount += newVal._3

        state.update((favCount, commCount, viewCount))
      })
      .timeout(Minutes(120))

    val actionBySite = siteActionRecordStream.transform(rdd => {
      rdd.toDF().createOrReplaceTempView("records")
      val actionsBySite = sqlc.sql(
        """SELECT site, timestampBucket,
            |SUM(CASE WHEN action = 'add_to_favorites' THEN 1 ELSE 0 END) as favCount,
            |SUM(CASE WHEN action = 'comment' THEN 1 ELSE 0 END) as commCount,
            |SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) as viewCount
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
    //mappedActionBySite.print(200)

    import com.metamx.tranquility.spark.BeamRDD._
    //mappedActionBySite.foreachRDD(rdd => rdd.propagate(new SiteActionRecordBeamFactory))

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
