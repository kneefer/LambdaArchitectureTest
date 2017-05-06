package com.sbartnik.layers.producer

import com.sbartnik.config.{AppConfig, ConfigurationProvider}
import com.sbartnik.common.Helpers._
import com.sbartnik.common.KafkaProducerOperations

import scala.util.Random

object WebTrafficDataProducer extends App {

  // Configurations
  val kafkaProducerConfigProps = ConfigurationProvider.kafkaProducer
  val kafkaTopic = AppConfig.Kafka.topic
  val kafkaNumOfPartitions = AppConfig.Kafka.numOfPartitions
  val generatorConfig = AppConfig.TrafficGen

  // Files with test inputs
  val Sites = getLinesFromResourceFile("sites.txt")
  val Referrers = getLinesFromResourceFile("referrers.txt")
  val Countries = getLinesFromResourceFile("countries.txt")

  // Dummy test inputs
  val visitorIds = (0 to generatorConfig.numOfVisitors).map(x => s"Visitor-$x")
  val subpagesIds= (0 to generatorConfig.numOfSubpages).map(x => s"Subpage-$x")

  val rnd = new Random()

  val kafkaOps = new KafkaProducerOperations(
    kafkaProducerConfigProps,
    kafkaTopic,
    kafkaNumOfPartitions)


}
