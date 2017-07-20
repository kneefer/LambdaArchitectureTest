package com.sbartnik.domain

case class ActionBySite(site: String,
                        timestamp_bucket: Long,
                        fav_count: Long,
                        comm_count: Long,
                        view_count: Long)
