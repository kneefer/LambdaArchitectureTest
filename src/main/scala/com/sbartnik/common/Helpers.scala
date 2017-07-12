package com.sbartnik.common

import java.util.{Collections, Properties}
import java.util.stream.Collectors

import net.liftweb.json.{DefaultFormats, JValue, Serialization, parse}

import scala.io.Source
import scala.language.implicitConversions

object Helpers {

  private implicit val formats = DefaultFormats

  def getLinesFromResourceFile(resFileName: String): Array[String] =
    Source.fromInputStream(getClass.getResourceAsStream(s"/$resFileName")).getLines.toArray

  def serialize[T <: AnyRef](value: T): String =
    Serialization.write(value)

  def deserialize[T <: AnyRef](value: String): T =
    Serialization.read(value)

  implicit def propertiesExtensions(properties: Properties) = new {
    def format: String = {
      properties.toString
    }
  }
}
