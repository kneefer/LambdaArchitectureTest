package com.sbartnik.domain

case class UniqueVisitorsBySite(site: String,
                                timestamp_bucket: Long,
                                unique_visitors: Long)
