package com.sbartnik.common

import com.datastax.driver.core.{Cluster, ConsistencyLevel, QueryOptions, Session}
import com.sbartnik.config.AppConfig

object CassandraOperations {

  private val conf = AppConfig.Cassandra

  val getInitializedSession: Session = {
    val cluster = new Cluster.Builder()
      .withClusterName("Lambda Architecture Test Cluster")
      .addContactPoint(conf.host)
      .withPort(conf.port)
      .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE)).build

    val session = cluster.connect
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${conf.keyspaceName} WITH REPLICATION = " +
      s"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

    session.execute(s"USE ${conf.keyspaceName}")

    session.execute(s"CREATE TABLE IF NOT EXISTS ${conf.batchUniqueVisitorsBySiteTable} (" +
      s"timestamp_bucket bigint, " +
      s"site text, " +
      s"unique_visitors bigint, " +
      s"PRIMARY KEY (site, timestamp_bucket)" +
      s") WITH CLUSTERING ORDER BY (timestamp_bucket DESC)")

    session.execute(s"CREATE TABLE IF NOT EXISTS ${conf.batchActionsBySiteTable} (" +
      s"timestamp_bucket bigint, " +
      s"site text," +
      s"fav_count bigint, " +
      s"comm_count bigint, " +
      s"view_count bigint, " +
      s"PRIMARY KEY (site, timestamp_bucket)" +
      s") WITH CLUSTERING ORDER BY (timestamp_bucket DESC)")

    session.execute(s"CREATE TABLE IF NOT EXISTS ${conf.speedUniqueVisitorsBySiteTable} (" +
      s"timestamp_bucket bigint, " +
      s"site text, " +
      s"unique_visitors bigint, " +
      s"PRIMARY KEY (site, timestamp_bucket)" +
      s") WITH CLUSTERING ORDER BY (timestamp_bucket DESC)")

    session.execute(s"CREATE TABLE IF NOT EXISTS ${conf.speedActionsBySiteTable} (" +
      s"timestamp_bucket bigint, " +
      s"site text," +
      s"fav_count bigint, " +
      s"comm_count bigint, " +
      s"view_count bigint, " +
      s"PRIMARY KEY (site, timestamp_bucket)" +
      s") WITH CLUSTERING ORDER BY (timestamp_bucket DESC)")

    session
  }
}
