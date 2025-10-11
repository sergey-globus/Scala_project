package org.example.service

import scala.collection.mutable
import org.example.domain._


object SessionProcessor {

  case class SessionStats(
                           qsCount: Int = 0,
                           cardCount: Int = 0,
                           TargetCardCount: Int = 0,
                           docOpens: Map[(String, String), Int] = Map.empty
                         )

  def processSession(session: Session, dropUnknownDates: Boolean = false): SessionStats = {
    val docCounts = mutable.Map.empty[(String, String), Int]

    session.docOpens.foreach { doc =>
      val date = doc.datetime
      val docId = doc.docId
      if (!dropUnknownDates || (date != "invalid" && docId != "unknown")) {
        docCounts((date, docId)) = docCounts.getOrElse((date, docId), 0) + 1
      }
    }

    SessionStats(
      qsCount = session.quickSearches.length,
      cardCount = session.cardSearches.length,
      TargetCardCount = session.cardSearches
        .flatMap(_.params)
        .count { case (num, text) => num == 0 && text.contains("ACC_45616") },
      docOpens = docCounts.toMap
    )
  }
}
