package com.sbartnik.domain

import com.sbartnik.common.Helpers
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

case class SiteActionRecord(timestamp: Long,
                            var timestampBucket: Long,
                            referrer: String,
                            action: String,
                            previousPage: String,
                            visitor: String,
                            geo: String,
                            timeSpentSeconds: Int,
                            subPage: String,
                            site: String,
                            var props: Map[String, String] = Map()) {

  def serialized: String = {
    s"$timestamp\t$referrer\t$action\t$previousPage\t$visitor\t$geo\t$timeSpentSeconds\t$subPage\t$site\n"
  }
}

object SiteActionRecord {

  def deserialize(line: String): Option[SiteActionRecord] = {
    val record = line.split("\\t")
    if (record.length == 9)
      Some(SiteActionRecord(
        record(0).toLong, -1, record(1), record(2), record(3),
        record(4), record(5), record(6).toInt, record(7), record(8)))
    else
      None
  }

  def fromStringRDDToRDD: (RDD[(String, String)]) => RDD[SiteActionRecord] = (input: RDD[(String, String)]) => {
    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges

    val BUCKET_MS_SIZE = 1000 * 60 * 60

    input.mapPartitionsWithIndex((index, iter) => {
      val or = offsetRanges(index)
      iter.flatMap(kv => {
        val siteActionRecord = SiteActionRecord.deserialize(kv._2) match {
          case Some(x) => {
            x.timestampBucket = x.timestamp / BUCKET_MS_SIZE * BUCKET_MS_SIZE
            x.props = Map(
              "topic" -> or.topic,
              "kafkaPartition" -> or.partition.toString,
              "fromOffset" -> or.fromOffset.toString,
              "untilOffset" -> or.untilOffset.toString)
            Some(x)
          }
          case None => None
        }
        siteActionRecord
      })
    })
  }
}