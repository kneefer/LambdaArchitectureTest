package com.sbartnik.domain

import java.sql.ResultSet
import com.datastax.driver.core.Row
import com.sbartnik.common.db.DbRowMapper

case class UniqueVisitorsBySite(site: String,
                                timestamp_bucket: Long,
                                unique_visitors: Long)

object UniqueVisitorsBySite extends DbRowMapper[UniqueVisitorsBySite] {
  override def mapSingleRecord(row: Row): UniqueVisitorsBySite = {
    UniqueVisitorsBySite(
      row.getString(0),
      row.getLong(1),
      row.getLong(2)
    )
  }

  override def mapSingleRecord(rs: ResultSet): UniqueVisitorsBySite = {
    UniqueVisitorsBySite(
      rs.getString(0),
      rs.getLong(1),
      rs.getLong(2)
    )
  }
}