package org.example.parser.model.events

import org.example.parser.model.DatetimeParser.parseDatetime
import org.example.parser.model.{Event, ParseContext, SearchEvent, SearchEventObject}

import java.time.LocalDateTime

case class QuickSearch(
                        override val id: String,
                        override val datetime: Option[LocalDateTime],
                        query: String,
                        override val foundDocs: Seq[String]
                      ) extends SearchEvent(id, datetime, foundDocs)

object QuickSearch extends SearchEventObject[QuickSearch] {

  override def addToSession(ctx: ParseContext, event: QuickSearch): Unit = {
    ctx.quickSearches += event
  }

  override protected val prefix = "QS"

  def parse(ctx: ParseContext): QuickSearch = {

    // --- QS datetime {query} ---
    var toks = ctx.curLine.split("\\s+", 3)
    var datetime = parseDatetime(toks(1))

    if (datetime.isEmpty) {      // Если поле datetime - пустое, берем из сессии
      ctx.logAcc.add(s"Bad datetime format in QS", ctx.fileName, toks(1))
      datetime = ctx.startDatetime
    }
    val rawQuery = toks(2)
    val query =
      if (rawQuery.startsWith("{") && rawQuery.endsWith("}")) {
        rawQuery.drop(1).dropRight(1)
      } else {
        ctx.logAcc.add(s"Bad query format in QS", ctx.fileName, ctx.curLine)
        "unknown"
      }

    // --- id Seq[Docs] ---
    ctx.curLine = ctx.lines.next()
    toks = ctx.curLine.split("\\s+")
    val id = toks.head
    val docs = toks.tail

    val event = QuickSearch(id, datetime, query, docs)
    addToContext(ctx, event)
    event
  }
}
