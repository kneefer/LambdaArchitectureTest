package com.sbartnik.layers.serving.logic

import com.sbartnik.common.CassandraOperations
import com.sbartnik.config.AppConfig
import com.sbartnik.domain.{ActionBySite, UniqueVisitorsBySite}
import org.apache.spark.streaming.{Milliseconds, Minutes}

import scala.collection.JavaConverters._
import scala.language.postfixOps

object LambdaBusinessLogic extends BusinessLogic with CassandraOperations {

  private val conf = AppConfig
  private def cs = getInitializedSession

  override def getSiteActions(siteName: String, windowSize: Int): List[ActionBySite] = {

    def timestampBucketBoundary = Milliseconds(System.currentTimeMillis) - (Minutes(conf.batchBucketMinutes) * windowSize.toInt)

    val filterExpressions = List(
      if(!siteName.isEmpty) s"site = '$siteName'" else "",
      if(windowSize > 0) s"timestamp_bucket > ${timestampBucketBoundary.milliseconds}" else ""
    ).filter(!_.isEmpty)

    val filterExpression = if(filterExpressions.nonEmpty)
      s"WHERE ${filterExpressions.mkString(" AND ")}"
    else
      ""

    val query = s"""SELECT site,
                   |    MIN(timestamp_bucket) AS timestamp_bucket,
                   |    SUM(comm_count) AS comm_count,
                   |    SUM(fav_count) AS fav_count,
                   |    SUM(view_count) AS view_count
                   |FROM ${conf.Cassandra.batchActionsBySiteTable}
                   |$filterExpression
                   |GROUP BY site
                   |ALLOW FILTERING;
      """.stripMargin

    val dbResultSet = cs.execute(query)

    val resultMapped = dbResultSet.all().asScala.toList.map(ActionBySite.map)
    resultMapped
  }

  override def getUniqueVisitors(siteName: String, windowIndex: Int): List[UniqueVisitorsBySite] = {
    val cs = getInitializedSession
    List(UniqueVisitorsBySite(s"lambda $siteName", windowIndex, 123))
  }
}
