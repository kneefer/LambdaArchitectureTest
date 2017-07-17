import sbt._

object Dependencies {

  val sparkVer = "2.0.0"

  val sparkCore =               "org.apache.spark" %% "spark-core" % sparkVer
  val sparkStreaming =          "org.apache.spark" %% "spark-streaming" % sparkVer
  val sparkStreamingKafka =     "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkVer
  val sparkHive =               "org.apache.spark" %% "spark-hive" % sparkVer
  val sparkSql =                "org.apache.spark" %% "spark-sql" % sparkVer
  val kafka =                   "org.apache.kafka" %% "kafka" % "0.8.2.1"
  val akkaHttp =                "com.typesafe.akka" %% "akka-http-experimental" % "2.4.11"
  val lift =                    "net.liftweb" %% "lift-json" % "2.6.2"
  val twitterStream =           "org.twitter4j" % "twitter4j-stream" % "4.0.4"
  val sparkCassandraConnect =   "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3"
  val cassandraDriver =         "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0"
  val logback =                 "ch.qos.logback" % "logback-classic" % "1.1.7"
  val akkaHttpJson =            "de.heikoseeberger" %% "akka-http-json4s" % "1.7.0"
  val json4s =                  "org.json4s" %% "json4s-native" % "3.3.0"
  val jansi =                   "org.fusesource.jansi" % "jansi" % "1.12"
  val tranquilityCore =         "io.druid" %% "tranquility-core" % "0.8.2"
  val tranquilitySpark =        "io.druid" %% "tranquility-spark" % "0.8.2"
}