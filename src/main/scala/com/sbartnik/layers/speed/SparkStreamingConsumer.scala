package com.sbartnik.layers.speed

import com.sbartnik.config.{AppConfig, ConfigurationProvider}
import com.sbartnik.domain.RandomRecord
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{SaveMode, SparkSession, TypedColumn}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreamingConsumer extends App {

  val kafkaConf = AppConfig.Kafka
  val kafkaDirectParams = Map(
    "metadata.broker.list" -> kafkaConf.Producer.bootstrapServers,
    "group.id" -> kafkaConf.HdfsConsumer.groupId,
    "auto.offset.reset" -> kafkaConf.HdfsConsumer.autoOffsetReset
  )

  val checkpointDirectory = "hdfs://localhost:9000/spark/checkpoints"
  val batchDuration = Seconds(5)
  val hdfsPath = "hdfs://localhost:9000/spark/data"

  val sparkSession = SparkSession
    .builder()
    .appName("Spark Streaming HDFS Consumer")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///${system:user.dir}/spark-warehouse")
    .getOrCreate()

  var sc = sparkSession.sparkContext
  sc.setCheckpointDir(checkpointDirectory)
  val sqlContext = sparkSession.sqlContext

  import sparkSession.implicits._

  def sparkStreamingCreateFunction(): StreamingContext = {
    val ssc = new StreamingContext(sc, batchDuration)

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaDirectParams, Set(kafkaConf.topic)
    )

    val randomRecordStream = kafkaDirectStream
      .transform(RandomRecord.fromStringRDDToRDD)
      .cache()

    randomRecordStream.foreachRDD(rdd => {
      val randomRecordDF = rdd
        .toDF()
        .selectExpr(
          "timestamp", "referrer", "action",
          "previousPage", "visitor", "geo",
          "timeSpentSeconds", "subPage", "site",
          "props.topic as topic",
          "props.kafkaPartition as kafkaPartition",
          "props.fromOffset as fromOffset",
          "props.untilOffset as untilOffset")

      randomRecordDF
        .write
        .partitionBy("topic", "kafkaPartition", "timestamp")
        .mode(SaveMode.Append)
        .parquet(hdfsPath)
    })

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
