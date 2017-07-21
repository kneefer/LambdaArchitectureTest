package com.sbartnik.common.db

import java.sql.{Connection, Driver, DriverManager}
import com.sbartnik.config.AppConfig

trait PostgresOperations {

  private val conf = AppConfig.Postgres

  def withConnection(func: Connection => Unit): Unit = {

    val connection = DriverManager.getConnection(conf.connectionString)
    try{
      func(connection)
    } finally {
      connection.close()
    }
  }

  def initDb(): Unit = withConnection(connection => {

  })
}
object PostgresOperations extends PostgresOperations