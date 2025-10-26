package org.example.parser.model

import org.example.checker.Validator.extractDatetime
import org.example.parser.model.events.{CardSearch, DocOpen, QuickSearch}
import org.reflections.Reflections

import java.time.LocalDateTime
import scala.collection.mutable
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
    val reflections = new Reflections("org.example.parser.model.events")

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

    // SESSION_START Datetime
    ctx.curLine = ctx.lines.next()
    if (ctx.curLine.startsWith(prefix)) {
      val toks = ctx.curLine.split("\\s+")
      ctx.startDatetime = extractDatetime(toks(1))
      if (ctx.startDatetime.isEmpty)
        ctx.logAcc.add(ctx.fileName, s"Bad datetime format: ${toks(1)}")
      ctx.curLine = ctx.lines.next()
    }
    else {
      ctx.logAcc.add(ctx.fileName, s"Not found SESSION_START: ${ctx.curLine}")
    }

    do {
      // --- Начало события ---
      allEventObjects.find(_.matches(ctx.curLine)) match {
        case Some(event) =>
          try {
            ctx += event.parse(ctx)
          } catch {
            case ex: Throwable =>
              ctx.logAcc.addException(ctx.fileName, ex, s"Parsing event failed on: ${ctx.curLine}")
          }
        case None => ctx.logAcc.add(ctx.fileName, s"Unknown event: ${ctx.curLine}")
      }

      ctx.curLine = if (ctx.lines.hasNext) ctx.lines.next() else null

    } while (ctx.curLine != null && !ctx.curLine.startsWith(postfix))

    // SESSION_END Datetime
    if (ctx.curLine != null) {
      val toks = ctx.curLine.split("\\s+")
      ctx.endDatetime = extractDatetime(toks(1))
      if (ctx.endDatetime.isEmpty)
        ctx.logAcc.add(ctx.fileName, s"Bad datetime format: ${toks(1)}")
      if (ctx.lines.hasNext)
        ctx.logAcc.add(ctx.fileName, s"[WARNING] Lines after SESSION_END")
    }
    else {
      ctx.logAcc.add(ctx.fileName, s"Not found SESSION_END: ${ctx.curLine}")
    }

    ctx.buildSession()
  }
}
