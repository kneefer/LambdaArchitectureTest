package com.sbartnik.layers.batch

import com.sbartnik.config.AppConfig
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

object BatchHdfsJob extends App {

  val conf = AppConfig
  val batchImagesPath = conf.hdfsBatchImagesPath

  val sparkConf = new SparkConf()
      .setAppName("Spark Batch Processing")
      .setMaster(AppConfig.sparkMaster)
      .set("spark.sql.warehouse.dir", "file:///${system:user.dir}/spark-warehouse")

  val sc = SparkContext.getOrCreate(sparkConf)
  val sqlc = SQLContext.getOrCreate(sc)

  val dfToProcess = sqlc.read.parquet(conf.hdfsDataPath)
      .where("unix_timestamp() - timestampBucket / 1000 <= 60 * 60 * 1")

  dfToProcess.createOrReplaceTempView("records")

  val uniqueVisitorsBySite = sqlc.sql(
    """SELECT site, timestampBucket,
        |COUNT(DISTINCT visitor) as uniqueVisitors
      |FROM records
      |GROUP BY site, timestampBucket
    """.stripMargin)

  //uniqueVisitorsBySite.show(500)

  uniqueVisitorsBySite
    .write
    .mode(SaveMode.Append)
    .partitionBy("timestampBucket")
    .parquet(batchImagesPath + "/uniqueVisitorsBySite")

  val actionsBySite = sqlc.sql(
    """SELECT site, timestampBucket,
        |SUM(CASE WHEN action = 'add_to_favorites' THEN 1 ELSE 0 END) as favCount,
        |SUM(CASE WHEN action = 'comment' THEN 1 ELSE 0 END) as commCount,
        |SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) as viewCount
      |FROM records
      |GROUP BY site, timestampBucket
    """.stripMargin
  ).cache()

  //actionsBySite.show(500)

  actionsBySite
    .write
    .mode(SaveMode.Append)
    .partitionBy("timestampBucket")
    .parquet(batchImagesPath + "/actionsBySite")
}
