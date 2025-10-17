package org.example.analysis

import org.apache.spark.rdd.RDD
import org.example.domain.Session

object ForDebug {
  def run(sessions: RDD[Session]): Unit = {
    val totalQS = sessions.map(_.quickSearches.length).sum().toLong
    val totalCardSearch = sessions.map(_.cardSearches.length).sum().toLong

    println(s"(debug) QS total: $totalQS")
    println(s"(debug) CARD_SEARCH total: $totalCardSearch")
  }
}
