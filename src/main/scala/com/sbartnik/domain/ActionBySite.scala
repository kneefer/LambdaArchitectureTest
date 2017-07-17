package com.sbartnik.domain

case class ActionBySite(site: String,
                        timestampBucket: Long,
                        favCount: Long,
                        commCount: Long,
                        viewCount: Long)
