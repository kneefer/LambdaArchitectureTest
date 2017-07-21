package com.sbartnik.domain

import com.datastax.driver.core.Row
import com.sbartnik.common.CassandraRowMapper

case class ActionBySite(site: String,
                        timestamp_bucket: Long,
                        fav_count: Long,
                        comm_count: Long,
                        view_count: Long)

object ActionBySite extends CassandraRowMapper[ActionBySite] {
  def mapRow(x: Row): ActionBySite = {
    ActionBySite(
      x.getString(0),
      x.getLong(1),
      x.getLong(2),
      x.getLong(3),
      x.getLong(4))
  }
}
