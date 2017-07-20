package com.sbartnik.layers.serving.logic

import com.sbartnik.common.CassandraOperations
import com.sbartnik.domain.{ActionBySite, UniqueVisitorsBySite}
import com.sbartnik.layers.serving.BusinessLogic

object LambdaBusinessLogic extends BusinessLogic with CassandraOperations {

  override def getSiteActions(siteName: String, windowSize: Long): List[ActionBySite] = {
    val cs = getInitializedSession
    List(ActionBySite(s"lambda ${siteName}", windowSize, 456, 789, 234))
  }

  override def getUniqueVisitors(siteName: String, windowIndex: Long): List[UniqueVisitorsBySite] = {
    val cs = getInitializedSession
    List(UniqueVisitorsBySite(s"lambda ${siteName}", windowIndex, 123))
  }
}
