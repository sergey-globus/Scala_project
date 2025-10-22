package org.example.model

import org.example.checker.Validator.{extractDatetime, isValidDocId}
import org.example.model.events.{CardSearch, DocOpen, QuickSearch}

import java.time.LocalDateTime
import scala.collection.mutable
import org.reflections.Reflections

import scala.jdk.CollectionConverters._

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

  private lazy val allEventObjects: Seq[EventObject[_ <: Event]] = {
    val reflections = new Reflections("org.example.model.events")

    reflections.getSubTypesOf(classOf[EventObject[_ <: Event]])
      .asScala
      .map(cls => cls.getField("MODULE$").get(null).asInstanceOf[EventObject[_ <: Event]])
      .toSeq
  }

  def empty(fileName: String): Session = Session(
    fileName,
    mutable.ListBuffer.empty[CardSearch],
    mutable.ListBuffer.empty[QuickSearch],
    mutable.ListBuffer.empty[DocOpen],
    None,
    None
  )

  def parse(ctx: ParseContext): Session = {

    var endFound = false
    while (ctx.lines.hasNext && !endFound) {
      ctx.curLine = ctx.lines.next()

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
                ctx.logAcc.addException(ctx.fileName, ex, s"Parsing event failed on: ${ctx.curLine}")
            }
          case None => ctx.logAcc.add(ctx.fileName, s"Unknown event: ${ctx.curLine}")
        }
      }

    }

    if (endFound && ctx.lines.hasNext)
      ctx.logAcc.add(ctx.fileName, s"[WARNING] Lines after SESSION_END")

    ctx.buildSession()
  }
}
