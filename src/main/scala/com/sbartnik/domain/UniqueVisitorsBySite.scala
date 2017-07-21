package com.sbartnik.domain

import com.datastax.driver.core.Row
import com.sbartnik.common.CassandraRowMapper

case class UniqueVisitorsBySite(site: String,
                                timestamp_bucket: Long,
                                unique_visitors: Long)

object UniqueVisitorsBySite extends CassandraRowMapper[UniqueVisitorsBySite] {
  override def mapRow(x: Row): UniqueVisitorsBySite = {
    UniqueVisitorsBySite(
      x.getString(0),
      x.getLong(1),
      x.getLong(2)
    )
  }
}