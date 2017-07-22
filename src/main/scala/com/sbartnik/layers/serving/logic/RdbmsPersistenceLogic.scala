package com.sbartnik.layers.serving.logic

import java.sql.ResultSet
import com.sbartnik.common.db.PostgresOperations
import com.sbartnik.domain.{ActionBySite, UniqueVisitorsBySite}
import com.sbartnik.common.db.DbRowMapper._

object RdbmsPersistenceLogic extends PersistenceLogic with PostgresOperations {

  override def getSiteActions(siteName: String, bucketsNumber: Int): List[ActionBySite] = {

    val dbQuery =
      s"""
         |
      """.stripMargin

    var dbResultSet: ResultSet = null
    withConnection(conn => {
      val ps = conn.prepareStatement(dbQuery)
      dbResultSet = ps.executeQuery()
    })

    val dbResultMapped = dbResultSet.map(ActionBySite)
    dbResultMapped
  }

  override def getUniqueVisitors(siteName: String, bucketIndex: Int): List[UniqueVisitorsBySite] = {

    val dbQuery =
      s"""
         |
      """.stripMargin

    var dbResultSet: ResultSet = null
    withConnection(conn => {
      val ps = conn.prepareStatement(dbQuery)
      dbResultSet = ps.executeQuery()
    })

    val dbResultMapped = dbResultSet.map(UniqueVisitorsBySite)
    dbResultMapped
  }
}
