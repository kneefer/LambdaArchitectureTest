package com.sbartnik.domain

import com.sbartnik.common.Helpers
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

case class RandomRecord(var timestamp: Long,
                        referrer: String,
                        action: String,
                        previousPage: String,
                        visitor: String,
                        geo: String,
                        timeSpentSeconds: Int,
                        subPage: String,
                        site: String,
                        var props: Map[String, String] = Map()) {
}

object RandomRecord {
  def fromStringRDDToRDD: (RDD[(String, String)]) => RDD[RandomRecord] = (input: RDD[(String, String)]) => {
    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges

    val BUCKET_MS_SIZE = 1000 * 60 * 60

    input.mapPartitionsWithIndex((index, iter) => {
      val or = offsetRanges(index)
      iter.map(kv => {
        val randomRecord = Helpers.deserialize[RandomRecord](kv._2)
        randomRecord.timestamp = randomRecord.timestamp / BUCKET_MS_SIZE * BUCKET_MS_SIZE
        randomRecord.props = Map(
          "topic" -> or.topic,
          "kafkaPartition" -> or.partition.toString,
          "fromOffset" -> or.fromOffset.toString,
          "untilOffset" -> or.untilOffset.toString)
        randomRecord
      })
    })
  }
}