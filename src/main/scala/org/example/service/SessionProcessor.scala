package org.example.service

import scala.collection.mutable
import org.example.infrastructure.Parser
import org.example.domain._

class SessionProcessor {
  private[SessionProcessor] val unknowns = mutable.ListBuffer.empty[String]
  private val unknownsSet = mutable.Set.empty[String]

  // Логируем только один раз
  private[SessionProcessor] def logUnknown(fileName: String, msg: String): Unit = synchronized {
    val entry = s"$fileName | $msg"
    if (!unknownsSet.contains(entry)) {
      unknowns += entry
      unknownsSet += entry
    }
  }

  private[SessionProcessor] def getUnknowns: List[String] = unknowns.toList
}

object SessionProcessor {
  private val comp = new SessionProcessor

  case class SessionStats(
                           qsCount: Int = 0,
                           cardCount: Int = 0,
                           TargetCardCount: Int = 0,
                           docOpens: Map[(String, String), Int] = Map.empty
                         )

  def processSession(lines: Iterable[(String, String)], dropUnknownDates: Boolean = false): SessionStats = {
    // Парсим сессию с нового Parser
    val session: Session = Parser.parseSession(lines, comp.logUnknown)

    val docCounts = mutable.Map.empty[(String, String), Int]
    session.docOpens.foreach { doc =>
      val date = doc.timestamp
      val docId = doc.docId

      if (!dropUnknownDates || date != "invalid" || docId != "unknown") {
        docCounts((date, docId)) = docCounts.getOrElse((date, docId), 0) + 1
      }
    }

    SessionStats(
      qsCount = session.qsQueries.length,
      cardCount = session.cardSearches.length,
      TargetCardCount = session.cardSearches
        .flatMap(_.params)
        .count { case (num, text) => num == 0 && text.contains("ACC_45616") },
      docOpens = docCounts.toMap
    )
  }

  def getUnknowns: List[String] = comp.getUnknowns
}
