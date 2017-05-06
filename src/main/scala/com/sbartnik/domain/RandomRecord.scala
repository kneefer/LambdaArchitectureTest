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
}
