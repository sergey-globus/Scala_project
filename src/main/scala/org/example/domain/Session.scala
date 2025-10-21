package org.example.domain

import org.example.domain.events.{CardSearch, DocOpen, QuickSearch}

import java.time.LocalDateTime
import scala.collection.mutable

import scala.util.Try


case class Session(
                    id: String,
                    cardSearches: Seq[CardSearch],
                    quickSearches: Seq[QuickSearch],
                    docOpens: Seq[DocOpen],
                    startDatetime: Option[LocalDateTime],
                    endDatetime: Option[LocalDateTime]
                  )

object Session {

  private val prefix = "SESSION_START"
  private val postfix = "SESSION_END"

    private val allEventObjects: Seq[EventObject[_ <: Event]] =
    Seq(CardSearch, QuickSearch, DocOpen)

  def empty(fileName: String): Session = Session(
    fileName,
    mutable.ListBuffer.empty[CardSearch],
    mutable.ListBuffer.empty[QuickSearch],
    mutable.ListBuffer.empty[DocOpen],
    None,
    None
  )

  def parse(
             fileName: String,
             lines: Iterator[String],
             isValidDocId: String => Boolean,
             extractDatetime: String => LocalDateTime,
             logUnknown: ((String, String)) => Unit,
             addException: (String, Throwable, String) => Unit
           ): Session = {

    val ctx = ParseContext(fileName, lines, isValidDocId, extractDatetime, logUnknown)

    // --- Обрабатываем события до SESSION_END ---
    var endFound = false
    while (lines.hasNext && !endFound) {
      ctx.curLine = lines.next()

      // SESSION_START Datetime
      if (ctx.curLine.startsWith(prefix)) {
        val toks = ctx.curLine.split("\\s+")
        ctx.startDatetime = Option(extractDatetime(toks(1)))
      }

      // SESSION_END Datetime
      else if (ctx.curLine.startsWith(postfix)) {
        val toks = ctx.curLine.split("\\s+")
        ctx.endDatetime = Option(extractDatetime(toks(1)))
        endFound = true
      }

      // --- Начало события ---
      else {
        allEventObjects.find(_.matches(ctx.curLine)) match {
          case Some(event) =>
            try {
              event.parse(ctx)
            } catch {
              case ex: Throwable =>
                addException(fileName, ex, s"Parsing event failed on: ${ctx.curLine}")
            }
          case None => logUnknown(fileName, s"Unknown event: ${ctx.curLine}")
        }
      }

    }

    if (endFound && lines.hasNext)
      logUnknown(fileName, s"[WARNING] Lines after SESSION_END")

    ctx.buildSession()
  }
}
