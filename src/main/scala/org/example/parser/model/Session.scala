package org.example.parser.model

import org.example.parser.model.DatetimeParser.parseDatetime
import org.example.parser.model.events.{CardSearch, DocOpen, QuickSearch}
import org.reflections.Reflections

import java.time.LocalDateTime
import scala.collection.mutable
import scala.jdk.CollectionConverters._
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

  private lazy val allEventObjects: Seq[EventObject[_ <: Event]] = {
    val reflections = new Reflections("org.example.parser.model.events")

    reflections.getSubTypesOf(classOf[EventObject[_ <: Event]])
      .asScala
      .map { cls =>
        Try(cls.getField("MODULE$").get(null).asInstanceOf[EventObject[_ <: Event]]).toOption
      }
      .collect { case Some(obj) => obj }
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
    try {
      // SESSION_START Datetime
      ctx.curLine = ctx.lines.next()
      if (ctx.curLine.startsWith(prefix)) {
        val toks = ctx.curLine.split("\\s+")
        ctx.startDatetime = parseDatetime(toks(1))
        if (ctx.startDatetime.isEmpty) {
          ctx.logAcc.add(s"Bad datetime format in SESSION_START", ctx.fileName, toks(1))
        }
        ctx.curLine = ctx.lines.next()
      } else {
        ctx.logAcc.add(s"Not found SESSION_START", ctx.fileName, ctx.curLine)
      }

      do {
        // --- Начало события ---
        allEventObjects.find(_.matches(ctx.curLine)) match {
          case Some(event) =>
            try {
              event.parse(ctx)
            } catch {
              case ex: Throwable =>
                val eventPrefix = ctx.curLine.split("\\s+").headOption.getOrElse("")
                ctx.logAcc.addException(ex, ctx.fileName, s"Parsing event $eventPrefix failed")
            }
          case None =>
            val eventPrefix = ctx.curLine.split("\\s+").headOption.getOrElse("")
            ctx.logAcc.add(s"Unknown event - $eventPrefix", ctx.fileName, "")
        }

        ctx.curLine = if (ctx.lines.hasNext) ctx.lines.next() else null

      } while (ctx.curLine != null && !ctx.curLine.startsWith(postfix))

      // SESSION_END Datetime
      if (ctx.curLine != null) {
        val toks = ctx.curLine.split("\\s+")
        ctx.endDatetime = parseDatetime(toks(1))
        if (ctx.endDatetime.isEmpty) {
          ctx.logAcc.add(s"Bad datetime format in SESSION_END", ctx.fileName, toks(1))
        }
        if (ctx.lines.hasNext) {
          ctx.logAcc.add(s"[WARNING] Lines after SESSION_END", ctx.fileName, "")
        }
      } else {
        ctx.logAcc.add(s"Not found SESSION_END", ctx.fileName, "")
      }

      ctx.buildSession()
    } catch {
      case ex: Throwable =>
        ctx.logAcc.addException(ex, ctx.fileName, "Parsing session failed")
        Session.empty(ctx.fileName)
    }
  }
}
