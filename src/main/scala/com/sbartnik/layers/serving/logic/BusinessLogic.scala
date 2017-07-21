package com.sbartnik.layers.serving.logic

import com.sbartnik.domain.{ActionBySite, UniqueVisitorsBySite}

trait BusinessLogic {

  def getSiteActions(siteName: String, windowSize: Int): List[ActionBySite]
  def getUniqueVisitors(siteName: String, windowIndex: Int): List[UniqueVisitorsBySite]
}
