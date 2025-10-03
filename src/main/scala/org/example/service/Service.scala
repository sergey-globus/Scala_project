package org.example.service

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.example.infrastructure.EventParser
import org.example.domain.Session

object Service {

  def parseSessions(sc: SparkContext, inputPath: String): RDD[Session] = {
    val lines = sc.textFile(inputPath)
    val sessionStartPrefix = "SESSION_START"
    val sessionEndPrefix = "SESSION_END"
    val qsPrefix = "QS"
    val cardStartPrefix = "CARD_SEARCH_START"
    val cardEndPrefix = "CARD_SEARCH_END"
    val docOpenPrefix = "DOC_OPEN"

    lines.mapPartitions { iter =>
      import scala.collection.mutable.ListBuffer
      var currentSession: Option[String] = None
      val out = ListBuffer.empty[Session]

      var docOpens = ListBuffer.empty[(String, String)]
      var qsCount = 0
      var cardCount = 0
      var acc45616Count = 0
      var inCard = false
      val cardBuf = ListBuffer.empty[String]

      while (iter.hasNext) {
        val line = iter.next().trim
        if (line.startsWith(sessionStartPrefix)) {
          currentSession.foreach { id =>
            out += Session(id, docOpens.toList, qsCount, cardCount, acc45616Count)
          }
          val parts = line.split("\\s+", 2)
          currentSession = Some(if (parts.length >= 2) parts(1) else s"sess_${java.util.UUID.randomUUID()}")
          docOpens.clear(); qsCount = 0; cardCount = 0; acc45616Count = 0
        } else if (line.startsWith(sessionEndPrefix)) {
          currentSession.foreach { id =>
            out += Session(id, docOpens.toList, qsCount, cardCount, acc45616Count)
          }
          currentSession = None
          docOpens.clear(); qsCount = 0; cardCount = 0; acc45616Count = 0
        } else {
          // обычная логика
          currentSession.foreach { _ =>
            if (line.startsWith(qsPrefix)) qsCount += 1
            else if (line.startsWith(cardStartPrefix)) { inCard = true; cardCount += 1; cardBuf.clear() }
            else if (line.startsWith(cardEndPrefix) && inCard) {
              inCard = false
              cardBuf.foreach(t => if (EventParser.norm(t) == "ACC_45616") acc45616Count += 1)
              cardBuf.clear()
            }
            else if (inCard) {
              val toks = line.split("\\s+")
              cardBuf ++= toks
            }
            else if (line.startsWith(docOpenPrefix)) {
              val toks = line.split("\\s+")
              if (toks.length >= 3) {
                val tsCandidate = toks(1)
                val docCandidate = toks.last
                if (EventParser.isLikelyDocId(docCandidate)) {
                  docOpens += ((EventParser.norm(docCandidate), tsCandidate))
                }
              }
            }
          }
        }
      }

      currentSession.foreach { id =>
        out += Session(id, docOpens.toList, qsCount, cardCount, acc45616Count)
      }

      out.iterator
    }
  }

  def aggregateResults(sessions: RDD[Session]): RDD[((String, String), Int)] = {
    sessions.flatMap { session =>
      val qsDocs = session.docOpens.map(_._1).toSet
      session.docOpens.map { case (docId, ts) =>
        val date = EventParser.extractDateFromTimestamp(ts)
        ((date, docId), 1)
      } ++ Seq(
        (("__META__", "QS_COUNT"), session.qsCount),
        (("__META__", "CARD_COUNT"), session.cardCount),
        (("__META__", "ACC_45616_CARD_SEARCH"), session.acc45616Count)
      )
    }.reduceByKey(_ + _)
  }
}
