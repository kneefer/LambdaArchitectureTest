package com.sbartnik.layers.speed

import com.sbartnik.config.{AppConfig, ConfigurationProvider}
import com.sbartnik.domain.RandomRecord
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{SaveMode, SparkSession, TypedColumn}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils

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

  var sc = ss.sparkContext
  sc.setCheckpointDir(checkpointDirectory)
  val sqlContext = ss.sqlContext

  import ss.implicits._

  def sparkStreamingCreateFunction(): StreamingContext = {
    val ssc = new StreamingContext(sc, streamingBatchDuration)

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
          "timestamp", "timestampBucket", "referrer", "action",
          "previousPage", "visitor", "geo",
          "timeSpentSeconds", "subPage", "site",
          "props.topic as topic",
          "props.kafkaPartition as kafkaPartition",
          "props.fromOffset as fromOffset",
          "props.untilOffset as untilOffset")

      randomRecordDF
        .write
        .partitionBy("topic", "kafkaPartition", "timestampBucket")
        .mode(SaveMode.Append)
        .parquet(dataPath)
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
