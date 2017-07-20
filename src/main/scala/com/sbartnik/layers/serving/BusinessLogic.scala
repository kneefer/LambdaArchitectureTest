package com.sbartnik.layers.serving

import com.sbartnik.domain.{ActionBySite, UniqueVisitorsBySite}

trait BusinessLogic {

  def getSiteActions(siteName: String, windowSize: Long): List[ActionBySite]
  def getUniqueVisitors(siteName: String, windowIndex: Long): List[UniqueVisitorsBySite]
}
