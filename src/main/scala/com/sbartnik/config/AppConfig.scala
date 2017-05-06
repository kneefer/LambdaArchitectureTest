package com.sbartnik.config

import com.typesafe.config.ConfigFactory

object AppConfig {

  private val appConfig = ConfigFactory.load()

  object Cassandra {
    private val cassandraConfig = appConfig.getConfig("cassandra")

    val port: Int = cassandraConfig.getInt("port")
    val hosts: java.util.List[String] = cassandraConfig.getStringList("hosts")
    val writeConsistency: String = cassandraConfig.getString("writeConsistency")
    val readConsistency: String = cassandraConfig.getString("readConsistency")
    val replicationFactor: Int = cassandraConfig.getInt("replicationFactor")
  }

  object Kafka {
    private val kafkaConfig = appConfig.getConfig("kafka")

    val topic: String = kafkaConfig.getString("topic")
    val numOfPartitions: Int = kafkaConfig.getInt("numOfPartitions")

    object Producer {
      private val kafkaProducerConfig = kafkaConfig.getConfig("producer")

      val clientId: String = kafkaProducerConfig.getString("clientId")
      val bootstrapServers: String = kafkaProducerConfig.getString("bootstrapServers")
      val acks: String = kafkaProducerConfig.getString("acks")
      val maxRetries: Int = kafkaProducerConfig.getInt("maxRetries")
      val batchSizeBytes: Int = kafkaProducerConfig.getInt("batchSizeBytes")
      val lingerTimeMs: Int = kafkaProducerConfig.getInt("lingerTimeMs")
      val bufferSizeBytes: Int = kafkaProducerConfig.getInt("bufferSizeBytes")
      val keySerializerClass: String = kafkaProducerConfig.getString("keySerializerClass")
      val valueSerializerClass: String = kafkaProducerConfig.getString("valueSerializerClass")
    }

    object Consumer {
      private val kafkaConsumerConfig = appConfig.getConfig("consumer")

      val groupId: String = kafkaConsumerConfig.getString("groupId")
      val clientId: String = kafkaConsumerConfig.getString("clientId")
      val zookeeperConnect: String = kafkaConsumerConfig.getString("zookeeperConnect")
      val enableAutoCommit: String = kafkaConsumerConfig.getString("enableAutoCommit")
      val autoOffsetReset: String = kafkaConsumerConfig.getString("autoOffsetReset")
      val consumerTimeoutMs: Int = kafkaConsumerConfig.getInt("consumerTimeoutMs")
      val autoCommitIntervalMs: Int = kafkaConsumerConfig.getInt("autoCommitIntervalMs")
      val keyDeserializerClass: String = kafkaConsumerConfig.getString("keyDeserializerClass")
      val valueDeserializerClass: String = kafkaConsumerConfig.getString("valueDeserializerClass")
    }

  }

  object TrafficGen {
    private val trafficGenConfig = appConfig.getConfig("trafficGen")

    val recordsPerFile: Int = trafficGenConfig.getInt("recordsPerFile")
    val numOfFiles: Int = trafficGenConfig.getInt("numOfFiles")
    val numOfVisitors: Int = trafficGenConfig.getInt("numOfVisitors")
    val timeMultiplier: Int = trafficGenConfig.getInt("timeMultiplier")
    val numOfSubpages: Int = trafficGenConfig.getInt("numOfSubpages")
  }
}
