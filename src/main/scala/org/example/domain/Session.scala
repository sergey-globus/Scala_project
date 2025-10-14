package org.example.domain

import java.time.LocalDateTime
import scala.collection.mutable

case class Session(
                    id: String,
                    cardSearches: Seq[CardSearch],
                    quickSearches: Seq[QuickSearch],
                    docOpens: Seq[DocOpen],
                    startDatetime: LocalDateTime,
                    endDatetime: LocalDateTime
                  )

object Session {

  private val prefix: String = "SESSION_START"
  private val postfix: String = "SESSION_END"

  private val allEvent: Seq[EventObject[_ <: Event]] =
    Seq(CardSearch, QuickSearch, DocOpen)

  def empty(fileName: String): Session = Session(
    fileName,
    mutable.ListBuffer.empty[CardSearch],
    mutable.ListBuffer.empty[QuickSearch],
    mutable.ListBuffer.empty[DocOpen],
    LocalDateTime.MIN,
    LocalDateTime.MIN
  )

  def parse(
             fileName: String,
             lines: Iterator[String],
             isValidDocId: String => Boolean,
             extractDatetime: String => Option[LocalDateTime],
             logUnknown: (String, String) => Unit
           ): Session = {

    val ctx = ParseContext(fileName, lines, isValidDocId, extractDatetime, logUnknown)

    // --- Обрабатываем события до SESSION_END ---
    var endFound = false
    while (lines.hasNext && !endFound) {
      ctx.curLine = lines.next().trim

      // SESSION_START Datetime
      if (ctx.curLine.startsWith(prefix)) {
        val toks = ctx.curLine.split("\\s+")
        if (toks.length != 2)
          logUnknown(fileName, s"Bad SESSION_START line format: ${ctx.curLine}")
        else
          ctx.startDatetime = extractDatetime(toks(1)).getOrElse {
            logUnknown(fileName, s"Invalid date: ${toks(1)}")
            LocalDateTime.MIN
          }
      }

      // SESSION_END Datetime
      else if (ctx.curLine.startsWith(postfix)) {
        val toks = ctx.curLine.split("\\s+")
        if (toks.length != 2)
          logUnknown(fileName, s"Bad SESSION_END line format: ${ctx.curLine}")
        else
          ctx.endDatetime = extractDatetime(toks(1)).getOrElse {
            logUnknown(fileName, s"Invalid date: ${toks(1)}")
            LocalDateTime.MIN
          }
        endFound = true
      }

      // --- Начало события ---
      else if (ctx.curLine.nonEmpty) {
        val maybeEvent = allEvent.collectFirst {
          case event if event.matches(ctx.curLine) =>
            // Парсер возвращает объект Event
            event.parse(ctx)
        }

        maybeEvent match {
          case Some(ev) => ev.addToSession(ctx)
          case None     => logUnknown(fileName, s"Unknown event: ${ctx.curLine}")
        }
      }
    }

    ctx.buildSession()
  }
}
