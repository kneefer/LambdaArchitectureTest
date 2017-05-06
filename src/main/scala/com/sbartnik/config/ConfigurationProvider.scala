package com.sbartnik.config

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

object ConfigurationProvider {
  def kafkaProducer: Properties = {
    val kafkaProducerConf = AppConfig.Kafka.Producer
    val props = new Properties()
    props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProducerConf.clientId)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerConf.bootstrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, kafkaProducerConf.acks)
    props.put(ProducerConfig.RETRIES_CONFIG, kafkaProducerConf.maxRetries)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProducerConf.batchSizeBytes)
    props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProducerConf.lingerTimeMs)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaProducerConf.bufferSizeBytes)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducerConf.keySerializerClass)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducerConf.valueSerializerClass)
    props
  }

  def kafkaConsumer: Properties = {
    val kafkaConsumerConf = AppConfig.Kafka.Consumer
    val props = new Properties()
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaConsumerConf.clientId)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerConf.groupId)
    props.put("zookeeper.connect",kafkaConsumerConf.zookeeperConnect)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaConsumerConf.enableAutoCommit)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConsumerConf.autoOffsetReset)
    props.put("consumer.timeout.ms", kafkaConsumerConf.consumerTimeoutMs)
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, kafkaConsumerConf.autoCommitIntervalMs)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaConsumerConf.keyDeserializerClass)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaConsumerConf.valueDeserializerClass)
    props
  }
}
