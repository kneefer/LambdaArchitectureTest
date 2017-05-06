package com.sbartnik.common

import scala.io.Source

object Helpers {
  def getLinesFromResourceFile(resFileName: String): Array[String] =
    Source.fromInputStream(getClass.getResourceAsStream(s"/$resFileName")).getLines.toArray
}
