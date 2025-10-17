package org.example.analysis

import org.apache.spark.rdd.RDD
import org.example.domain.Session

object Task1 {
  def run(sessions: RDD[Session]): Unit = {
    val totalTarget = sessions.map { session =>
      session.cardSearches
        .flatMap(_.params)
        .count { case (num, text) => num == 0 && text.contains("ACC_45616") }
    }.sum().toLong

    println(s"(Task1) ACC_45616 CARD_SEARCH count: $totalTarget")
  }
}
