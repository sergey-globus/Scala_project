package org.example.analysis

import org.apache.spark.rdd.RDD
import org.example.model.Session

object Task1 {
  def run(sessions: RDD[Session]): Unit = {
    val totalTarget = sessions.map { session =>
      session.cardSearches
        .flatMap(_.params)
        .count { case (num, text) => (num == 0 || num == 134) &&
          (text.contains("ACC_45616") || text.contains("acc_45616") ||
            text.contains("АСС_45616") || text.contains("асс_45616")) }  // на русском
    }.sum().toLong

    println(s"(Task1) ACC_45616 CARD_SEARCH count: $totalTarget")
  }
}
