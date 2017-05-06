package com.sbartnik.domain

case class RandomRecord(timestamp: Long,
                        referrer: String,
                        action: String,
                        previousPage: String,
                        visitor: String,
                        geo: String,
                        timeSpentSeconds: Int,
                        subPage: String,
                        site: String) {

  def convertToLine: String = {
    val line = s"$timestamp\t$referrer\t$action\t$previousPage\t$visitor\t$geo\t$timeSpentSeconds\t$subPage\t$site\n"
    line
  }
}
