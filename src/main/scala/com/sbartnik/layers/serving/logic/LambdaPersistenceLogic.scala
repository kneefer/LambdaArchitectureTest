package com.sbartnik.layers.serving.logic

import com.sbartnik.common.CassandraOperations
import com.sbartnik.config.AppConfig
import com.sbartnik.domain.{ActionBySite, UniqueVisitorsBySite}
import com.sbartnik.common.CassandraRowMapper._

import scala.language.postfixOps

object LambdaPersistenceLogic extends PersistenceLogic with CassandraOperations {

  private val conf = AppConfig
  private val cs = getInitializedSession

  private def timestampBucketBoundary(index: Int) = {
    System.currentTimeMillis - (conf.batchBucketMinutes * 60 * 1000 * index)
  }

  override def getSiteActions(siteName: String, bucketsNumber: Int): List[ActionBySite] = {

    val batchFilters = List(
      if(!siteName.isEmpty) Some(s"site = '$siteName'") else None,
      if(bucketsNumber > 0) Some(s"timestamp_bucket > ${timestampBucketBoundary(bucketsNumber)}") else None
    ).filter(_.isDefined).map(_.get)

    val batchFilterExpression = batchFilters.mkString(if(batchFilters.isEmpty) "" else "WHERE ", " AND ", "")
    val speedFilterExpression = if(!siteName.isEmpty) s"WHERE site = '$siteName'" else ""

    val batchQuery =
      s"""SELECT site,
           |MIN(timestamp_bucket) AS timestamp_bucket,
           |SUM(comm_count) AS comm_count,
           |SUM(fav_count) AS fav_count,
           |SUM(view_count) AS view_count
         |FROM ${conf.Cassandra.batchActionsBySiteTable}
         |$batchFilterExpression
         |GROUP BY site
         |ALLOW FILTERING;
      """.stripMargin

    val speedQuery =
      s"""SELECT *
         |FROM ${conf.Cassandra.speedActionsBySiteTable}
         |$speedFilterExpression
         |PER PARTITION LIMIT 1;
      """.stripMargin

    val batchDbResultSet = cs.execute(batchQuery)
    val speedDbResultSet = cs.execute(speedQuery)

    val batchResultMapped = batchDbResultSet.map(ActionBySite)
    val speedResultMapped = speedDbResultSet.map(ActionBySite)

    val mergedResult = (batchResultMapped ++ speedResultMapped)
      .groupBy(_.site).map(x => ActionBySite(
        x._1,
        x._2.head.timestamp_bucket,
        x._2.map(_.comm_count).sum,
        x._2.map(_.fav_count).sum,
        x._2.map(_.view_count).sum)
      )

    mergedResult.toList
  }

  override def getUniqueVisitors(siteName: String, bucketIndex: Int): List[UniqueVisitorsBySite] = {

    val batchFilters = List(
      if(!siteName.isEmpty) Some(s"site = '$siteName'") else None,
      if(bucketIndex > 0) Some(s"timestamp_bucket < ${timestampBucketBoundary(bucketIndex)}") else None
    ).filter(_.isDefined).map(_.get)

    val batchFilterExpression = batchFilters.mkString(if(batchFilters.isEmpty) "" else "WHERE ", " AND ", "")
    val speedFilterExpression = if(!siteName.isEmpty) s"WHERE site = '$siteName'" else ""

    val batchQuery =
      s"""SELECT *
         |FROM ${conf.Cassandra.batchUniqueVisitorsBySiteTable}
         |$batchFilterExpression
         |PER PARTITION LIMIT 1
         |ALLOW FILTERING;
      """.stripMargin

    val speedQuery =
      s"""SELECT *
         |FROM ${conf.Cassandra.speedUniqueVisitorsBySiteTable}
         |$speedFilterExpression
         |PER PARTITION LIMIT 1;
      """.stripMargin

    val dbResult = cs.execute(if(bucketIndex > 0) batchQuery else speedQuery)
    val dbResultMapped = dbResult.map(UniqueVisitorsBySite)

    dbResultMapped
  }
}
