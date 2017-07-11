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

    object HdfsConsumer {
      private val fileSystemConsumerConfig = appConfig.getConfig("hdfsConsumer")

      val groupId: String = fileSystemConsumerConfig.getString("groupId")
      val clientId: String = fileSystemConsumerConfig.getString("clientId")
      val zookeeperConnect: String = fileSystemConsumerConfig.getString("zookeeperConnect")
      val enableAutoCommit: String = fileSystemConsumerConfig.getString("enableAutoCommit")
      val autoOffsetReset: String = fileSystemConsumerConfig.getString("autoOffsetReset")
      val consumerTimeoutMs: Int = fileSystemConsumerConfig.getInt("consumerTimeoutMs")
      val autoCommitIntervalMs: Int = fileSystemConsumerConfig.getInt("autoCommitIntervalMs")
      val keyDeserializerClass: String = fileSystemConsumerConfig.getString("keyDeserializerClass")
      val valueDeserializerClass: String = fileSystemConsumerConfig.getString("valueDeserializerClass")
    }

    object BatchConsumer {
      private val fileSystemConsumerConfig = appConfig.getConfig("batchConsumer")

      val groupId: String = fileSystemConsumerConfig.getString("groupId")
      val clientId: String = fileSystemConsumerConfig.getString("clientId")
      val zookeeperConnect: String = fileSystemConsumerConfig.getString("zookeeperConnect")
      val enableAutoCommit: String = fileSystemConsumerConfig.getString("enableAutoCommit")
      val autoOffsetReset: String = fileSystemConsumerConfig.getString("autoOffsetReset")
      val consumerTimeoutMs: Int = fileSystemConsumerConfig.getInt("consumerTimeoutMs")
      val autoCommitIntervalMs: Int = fileSystemConsumerConfig.getInt("autoCommitIntervalMs")
      val keyDeserializerClass: String = fileSystemConsumerConfig.getString("keyDeserializerClass")
      val valueDeserializerClass: String = fileSystemConsumerConfig.getString("valueDeserializerClass")
    }
  }

  object TrafficGen {
    private val trafficGenConfig = appConfig.getConfig("trafficGen")

    val recordsPerFile: Int = trafficGenConfig.getInt("recordsPerFile")
    val numOfFiles: Int = trafficGenConfig.getInt("numOfFiles")
    val numOfVisitors: Int = trafficGenConfig.getInt("numOfVisitors")
    val timeMultiplier: Int = trafficGenConfig.getInt("timeMultiplier")
    val numOfSubpages: Int = trafficGenConfig.getInt("numOfSubpages")
    val sleepAfterEachFileMs: Int = trafficGenConfig.getInt("sleepAfterEachFileMs")
  }
}
