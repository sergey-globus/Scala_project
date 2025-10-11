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

  private class InvalidSession(msg: String) extends Exception(msg)

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

    // --- Проверка первой строки: SESSION_START ---
    if (!lines.hasNext) throw new InvalidSession("Empty file, no SESSION_START found")
    val firstLine = lines.next().trim
    if (!firstLine.startsWith(prefix))
      throw new InvalidSession(s"First line must be started SESSION_START, found line: $firstLine")

    val session = new Session(fileName)

    // SESSION_START Datetime
    val startToks = firstLine.split("\\s+")
    session.startDatetime = startToks.lift(1).getOrElse("unknown")
    if (startToks.length > 2)
      logUnknown(fileName, s"Bad SESSION_START line format: $firstLine")
    if (session.startDatetime == "invalid")
      logUnknown(fileName, s"Invalid date: ${session.startDatetime}")

    // --- Обрабатываем события до SESSION_END ---
    var endFound = false
    while (lines.hasNext && !endFound) {
      val line = lines.next().trim

      // SESSION_END Datetime
      if (line.startsWith(postfix)) {
        val endToks = line.split("\\s+")
        session.endDatetime = endToks.lift(1).getOrElse("unknown")
        if (endToks.length > 2) logUnknown(fileName, s"Bad SESSION_END line format: $line")
        if (session.endDatetime == "invalid") logUnknown(fileName, s"Invalid date: ${session.endDatetime}")
        endFound = true
      }

      // --- Начало события ---
      else if (line.nonEmpty) {
        val maybeEvent = allEvent.collectFirst {
          case event if event.matches(line) =>
            // Парсер возвращает объект Event
            event.parse(fileName, line, lines, isValidDocId, extractDateFromDatetime, logUnknown)
        }

        maybeEvent match {
          case Some(ev) => ev.addToSession(session)
          case None     => logUnknown(fileName, s"Unknown event: $line")
        }
      }
    }

    // --- Проверка: SESSION_END должна быть ---
    if (!endFound)
      throw new InvalidSession("SESSION_END not found at the end of file")

    session
  }
}
