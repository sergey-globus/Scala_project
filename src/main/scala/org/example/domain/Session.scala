package org.example.domain

import scala.collection.mutable.ListBuffer

case class Session(
                    id: String,
                    cardSearches: ListBuffer[CardSearch] = ListBuffer.empty[CardSearch],
                    quickSearches: ListBuffer[QuickSearch] = ListBuffer.empty[QuickSearch],
                    docOpens: ListBuffer[DocOpen] = ListBuffer.empty[DocOpen],
                    var startDatetime: String = "unknown",
                    var endDatetime: String = "unknown"
                  )

object Session {

  private val prefix: String = "SESSION_START"
  private val postfix: String = "SESSION_END"

  private val allEvent: Seq[EventObject[_ <: Event]] =
    Seq(CardSearch, QuickSearch, DocOpen)

  def empty(fileName: String): Session = Session(fileName)

  def parse(
             fileName: String,
             lines: Iterator[String],
             isValidDocId: String => Boolean,
             extractDateFromDatetime: String => String,
             logUnknown: (String, String) => Unit
           ): Session = {

    val session = new Session(fileName)

    while (lines.hasNext) {
      val line = lines.next().trim

      // SESSION_START Datetime
      if (line.startsWith(prefix)) {
        val toks = line.split("\\s+")
        val dt = toks.lift(1).getOrElse("unknown")
        session.startDatetime = dt
        if (dt == "invalid") logUnknown(fileName, s"Invalid date: $dt")
        if (toks.length > 2) logUnknown(fileName, s"Bad SESSION_START line format: $line")
      }

      // SESSION_END Datetime
      else if (line.startsWith(postfix)) {
        val toks = line.split("\\s+")
        val dt = toks.lift(1).getOrElse("unknown")
        session.endDatetime = dt
        if (dt == "invalid") logUnknown(fileName, s"Invalid date: $dt")
        if (toks.length > 2) logUnknown(fileName, s"Bad SESSION_END line format: $line")
      }

      // начало события
      else if (line.nonEmpty) {
        val maybeEvent = allEvent.collectFirst {
          case event if event.matches(line) =>
            // парсер возвращает объект Event
            event.parse(fileName, line, lines, isValidDocId, extractDateFromDatetime, logUnknown)
        }

        maybeEvent match {
          case Some(ev) => ev.addToSession(session)
          case None     => logUnknown(fileName, s"Unknown event: $line")
        }
      }

    }

    session
  }
}
