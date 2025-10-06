package org.example.infrastructure

import scala.collection.mutable.ListBuffer

object SessionExtractor {
  def extractSessions(
                       lines: Iterator[(String, String)],
                       sessionStartPrefix: String = "SESSION_START",
                       sessionEndPrefix: String = "SESSION_END"
                     ): Iterator[(String, (String, String))] = {
    var currentSession: Option[String] = None
    val out = ListBuffer.empty[(String, (String, String))]
    while (lines.hasNext) {
      val (fileName, lineRaw) = lines.next()
      val line = Option(lineRaw).getOrElse("").trim
      if (line.startsWith(sessionStartPrefix)) {
        val parts = line.split("\\s+", 2)
        currentSession = Some(
          if (parts.length >= 2 && parts(1).nonEmpty)
            parts(1)
          else "session_unknown_" + java.util.UUID.randomUUID()
        )
      } else if (line.startsWith(sessionEndPrefix)) {
        currentSession = None
      } else {
        currentSession.foreach(id => out += ((id, (fileName, line))))
      }
    }
    out.iterator
  }
}
