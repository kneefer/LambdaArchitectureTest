package com.sbartnik.common

import scala.collection.JavaConverters._
import com.datastax.driver.core.{ResultSet, Row}

trait CassandraRowMapper[T] {
  def mapRow(row: Row): T
}

object CassandraRowMapper {
  implicit class ResultSetExtensions(val resultSet: ResultSet) extends AnyVal {
    def map[T](mapper: CassandraRowMapper[T]): List[T] = {
      resultSet.all().asScala.toList.map(mapper.mapRow)
    }
  }
}