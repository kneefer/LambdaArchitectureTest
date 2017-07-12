package com.sbartnik.domain

import org.apache.spark.rdd.RDD

case class RandomRecord(timestamp: Long,
                        referrer: String,
                        action: String,
                        previousPage: String,
                        visitor: String,
                        geo: String,
                        timeSpentSeconds: Int,
                        subPage: String,
                        site: String,
                        props: Map[String, String] = Map()) {
}

object RandomRecord {
  def fromStringRDDToRDD: (RDD[(String, String)]) => RDD[RandomRecord] = (input: RDD[(String, String)]) => {
    input.map(_ => RandomRecord(1, "", "", "", "", "", 1, "", ""))
  }
}