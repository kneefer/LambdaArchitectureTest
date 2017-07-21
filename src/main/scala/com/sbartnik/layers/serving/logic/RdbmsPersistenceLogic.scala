package com.sbartnik.layers.serving.logic

import com.sbartnik.common.db.PostgresOperations
import com.sbartnik.domain.{ActionBySite, UniqueVisitorsBySite}

object RdbmsPersistenceLogic extends PersistenceLogic with PostgresOperations {

  override def getSiteActions(siteName: String, bucketsNumber: Int): List[ActionBySite] = {
    withConnection(conn => {

    })
    null
  }

  override def getUniqueVisitors(siteName: String, bucketIndex: Int): List[UniqueVisitorsBySite] = {
    withConnection(conn => {

    })
    null
  }
}
