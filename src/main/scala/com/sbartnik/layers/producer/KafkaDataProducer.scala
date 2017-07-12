package com.sbartnik.layers.producer

import com.sbartnik.config.{AppConfig, ConfigurationProvider}
import com.sbartnik.common.Helpers._
import com.sbartnik.common.KafkaProducerOperations
import com.sbartnik.domain.RandomRecord
import org.slf4j.LoggerFactory
import scala.util.Random

object KafkaDataProducer extends App {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Configurations
  val kafkaProducerConfigProps = ConfigurationProvider.kafkaProducer
  val kafkaTopic = AppConfig.Kafka.topic
  val kafkaNumOfPartitions = AppConfig.Kafka.numOfPartitions
  val generatorConfig = AppConfig.TrafficGen

  // Files with test inputs
  val sites = getLinesFromResourceFile("sites.txt")
  val referrers = getLinesFromResourceFile("referrers.txt")
  val countries = getLinesFromResourceFile("countries.txt")

  // Dummy test inputs
  val visitorsIds = (0 to generatorConfig.numOfVisitors).map(x => s"Visitor-$x")
  val subpagesIds= (0 to generatorConfig.numOfSubpages).map(x => s"Subpage-$x")

  val rnd = new Random()

  val kafkaOps = new KafkaProducerOperations(
    kafkaProducerConfigProps,
    kafkaTopic,
    kafkaNumOfPartitions)

  for (currFileNum <- 1 to generatorConfig.numOfFiles) {

    val randomSleepEvery = rnd.nextInt(generatorConfig.recordsPerFile - 1) + 1
    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (currRecordIndex <- 1 to generatorConfig.recordsPerFile) {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis - timestamp) * generatorConfig.timeMultiplier)
      timestamp = System.currentTimeMillis

      val record = getRandomRecord(adjustedTimestamp, currRecordIndex)
      val recordAsLine = serialize(record)
      kafkaOps.send(recordAsLine)

      if(currRecordIndex % randomSleepEvery == 0) {
        logger.info(s"Sent $currRecordIndex messages")
        val sleepTime = rnd.nextInt(randomSleepEvery * 60)
        logger.info(s"Sleeping for $sleepTime ms")
        Thread.sleep(sleepTime)
      }
    }

    val sleepAfterEachFile = generatorConfig.sleepAfterEachFileMs
    logger.info(s"Sleeping for $sleepAfterEachFile ms")
    Thread.sleep(sleepAfterEachFile)
  }

  def getRandomRecord(timestamp: Long, currRecordIndex: Int): RandomRecord = {
    val action = currRecordIndex % (rnd.nextInt(generatorConfig.recordsPerFile) + 1) match {
      case 0 => "add_to_favorites"
      case 1 => "comment"
      case _ => "page_view"
    }

    val geo = rnd.nextInt(3) match {
      case 0 => countries(rnd.nextInt(countries.length - 1))
      case _ => ""
    }

    val referrer = referrers(rnd.nextInt(referrers.length - 1))

    val previousPage = referrer match {
      case "Internal" => subpagesIds(rnd.nextInt(subpagesIds.length - 1))
      case "Other" => sites(rnd.nextInt(sites.length - 1))
      case _ => ""
    }

    val timeSpentSeconds = rnd.nextInt(5 * 60) + 1

    val visitor = visitorsIds(rnd.nextInt(visitorsIds.length - 1))
    val subpage = subpagesIds(rnd.nextInt(subpagesIds.length - 1))
    val site = sites(rnd.nextInt(sites.length - 1))

    val randomRecord = RandomRecord(
      timestamp, referrer, action, previousPage, visitor, geo, timeSpentSeconds, subpage, site
    )
    randomRecord
  }
}
