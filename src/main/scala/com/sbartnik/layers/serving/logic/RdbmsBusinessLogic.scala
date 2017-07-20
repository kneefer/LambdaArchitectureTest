package com.sbartnik.layers.serving.logic

import com.sbartnik.domain.{ActionBySite, UniqueVisitorsBySite}
import com.sbartnik.layers.serving.BusinessLogic

object RdbmsBusinessLogic extends BusinessLogic {

  override def getSiteActions(siteName: String, windowSize: Long): List[ActionBySite] = {
    List(ActionBySite(s"rdbms ${siteName}", windowSize, 456, 789, 234))
  }

  override def getUniqueVisitors(siteName: String, windowIndex: Long): List[UniqueVisitorsBySite] = {
    List(UniqueVisitorsBySite(s"rdbms ${siteName}", windowIndex, 123))
  }
}
