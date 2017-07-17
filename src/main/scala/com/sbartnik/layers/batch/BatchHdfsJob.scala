package com.sbartnik.layers.batch

import com.sbartnik.config.AppConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

object BatchHdfsJob extends App {

  val conf = AppConfig
  val batchImagesPath = conf.hdfsBatchImagesPath

  val ss = SparkSession
    .builder()
    .appName("Spark Batch Processing")
    .master(AppConfig.sparkMaster)
    .config("spark.sql.warehouse.dir", "file:///${system:user.dir}/spark-warehouse")
    .getOrCreate()

  val sc = ss.sparkContext
  val sqlc = ss.sqlContext

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
