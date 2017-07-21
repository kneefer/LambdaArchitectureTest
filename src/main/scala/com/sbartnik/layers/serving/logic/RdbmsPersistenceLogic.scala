package com.sbartnik.layers.serving.logic

import com.sbartnik.domain.{ActionBySite, UniqueVisitorsBySite}

object RdbmsPersistenceLogic extends PersistenceLogic {

  override def getSiteActions(siteName: String, bucketsNumber: Int): List[ActionBySite] = {
    List(ActionBySite(s"rdbms $siteName", bucketsNumber, 456, 789, 234))
  }

  override def getUniqueVisitors(siteName: String, bucketIndex: Int): List[UniqueVisitorsBySite] = {
    List(UniqueVisitorsBySite(s"rdbms $siteName", bucketIndex, 123))
  }
}
