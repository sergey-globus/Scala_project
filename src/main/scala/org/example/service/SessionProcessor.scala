package org.example.service

import scala.collection.mutable
import org.example.parser.LogParser

class SessionProcessor {
  private[SessionProcessor] val unknowns = mutable.ListBuffer.empty[String]

  private[SessionProcessor] def logUnknown(msg: String): Unit = synchronized {
    unknowns += msg
  }

  private[SessionProcessor] def getUnknowns: List[String] = unknowns.toList
}

object SessionProcessor {
  private val comp = new SessionProcessor

  case class SessionStats(
                           qsCount: Int = 0,
                           cardCount: Int = 0,
                           acc45616CardCount: Int = 0,
                           docOpens: Map[(String, String), Int] = Map.empty
                         )

  private def isValidDocId(token: String): Boolean = {
    token != null && token.matches("""[A-Z]{1,5}_\d{1,7}""")
  }

  def processSession(
                      lines: Iterable[(String, String)],
                      dropUnknownDates: Boolean = false
                    ): SessionStats = {
    val qsDocs = mutable.Set.empty[String]
    var localQSCount, localCardCount, acc45616CardSearchCount = 0
    var inCard = false
    val cardBuf = mutable.ListBuffer.empty[String]
    val docOpens = mutable.ListBuffer.empty[(String, String)] // (docId, ts)

    for ((fileName, raw) <- lines) {
      val line = Option(raw).getOrElse("").trim
      if (line.startsWith("QS")) {
        localQSCount += 1
        val tokens = line.split("\\s+").drop(1)
        var skipBlock = false
        for (t <- tokens) {
          if (t.startsWith("{")) skipBlock = true
          if (!skipBlock && isValidDocId(t)) qsDocs += t
          else if (!skipBlock && t.nonEmpty) comp.logUnknown(s"$fileName | Unknown QS docId: $t")
          if (t.endsWith("}")) skipBlock = false
        }
      } else if (line.startsWith("CARD_SEARCH_START")) {
        inCard = true
        localCardCount += 1
        cardBuf.clear()
      } else if (line.startsWith("CARD_SEARCH_END") && inCard) {
        inCard = false
        cardBuf.foreach { d =>
          if (isValidDocId(d)) qsDocs += d
          else comp.logUnknown(s"$fileName | Unknown CARD docId: $d")
        }
        cardBuf.clear()
      } else if (inCard) {
        if (line.startsWith("$0") && line.contains("ACC_45616")) acc45616CardSearchCount += 1
        line.split("\\s+").foreach(t => cardBuf += t)
      } else if (line.startsWith("DOC_OPEN")) {
        val toks = line.split("\\s+")
        if (toks.length >= 3) {
          val tsCandidate = toks(1)
          val docCandidate = toks.last
          if (isValidDocId(docCandidate)) {
            docOpens += ((docCandidate, tsCandidate))
          } else comp.logUnknown(s"$fileName | Bad DOC_OPEN docId: $line")
        } else comp.logUnknown(s"$fileName | Bad DOC_OPEN too few tokens: $line")
      }
    }

    val docCounts = mutable.Map.empty[(String, String), Int]
    docOpens.foreach { case (docId, ts) =>
      val date = LogParser.extractDateFromTimestamp(ts)
      if (!dropUnknownDates || date != "unknown")
        docCounts((date, docId)) = docCounts.getOrElse((date, docId), 0) + 1
      if (date == "unknown") comp.logUnknown(s"Unknown date for doc: $docId ts: $ts")
    }

    SessionStats(localQSCount, localCardCount, acc45616CardSearchCount, docCounts.toMap)
  }

  def getUnknowns: List[String] = comp.getUnknowns
}
