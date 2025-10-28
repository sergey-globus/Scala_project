package org.example.parser.model.events

import org.example.parser.model.DatetimeParser.parseDatetime
import org.example.parser.model.{Event, EventObject, ParseContext}

import java.time.LocalDateTime

case class DocOpen(
                    override val datetime: Option[LocalDateTime],
                    searchId: String,
                    docId: String
                  ) extends Event(datetime)

object DocOpen extends EventObject[DocOpen] {

  override def addToSession(ctx: ParseContext, event: DocOpen): Unit = {
    ctx.docOpens += event
  }

  override def addToContext(ctx: ParseContext, event: DocOpen): Unit = {
    super.addToContext(ctx, event)
    ctx.attachDocOpenToSearch(event.datetime, event.docId, event.searchId)
  }

  override val prefix = "DOC_OPEN"

  def parse(ctx: ParseContext): DocOpen = {

    // DOC_OPEN [datetime] searchId docId
    val toks = ctx.curLine.split("\\s+")
    val (datetime, searchId, docId) = toks.length match {
      case 4 =>      // формат с датой
        val dt = parseDatetime(toks(1))
        if (dt.isEmpty) {
          ctx.logAcc.add(s"Bad datetime format in DOC_OPEN", ctx.fileName, toks(1))
        }
        (dt, toks(2), toks(3))
      case 3 =>     // формат без даты
        (None, toks(1), toks(2))
    }

    val event = DocOpen(datetime, searchId, docId)
    addToContext(ctx, event)
    event
  }
}
