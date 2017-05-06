package com.sbartnik.common

import net.liftweb.json.{JValue, Serialization, parse}

import scala.io.Source

object Helpers {

  def getLinesFromResourceFile(resFileName: String): Array[String] =
    Source.fromInputStream(getClass.getResourceAsStream(s"/$resFileName")).getLines.toArray

  def serialize[T <: AnyRef](value: T): String =
    Serialization.write(value)

  protected def deserialize(value: String): JValue =
    parse(value)
}
