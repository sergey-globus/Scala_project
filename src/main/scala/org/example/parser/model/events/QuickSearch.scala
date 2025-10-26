package org.example.parser.model.events

import org.example.checker.Validator.extractDatetime
import org.example.parser.model.{EventObject, ParseContext, SearchEvent}

import java.time.LocalDateTime

case class QuickSearch(
                        override val id: String,
                        override val datetime: Option[LocalDateTime],
                        query: String,
                        override val foundDocs: Seq[String]
                      ) extends SearchEvent(id, datetime, foundDocs) {

  override def addToSession(ctx: ParseContext): Unit =
    ctx.quickSearches += this
}

object QuickSearch extends EventObject[QuickSearch] {

  override protected val prefix = "QS"

  def parse(ctx: ParseContext): QuickSearch = {

    // --- QS datetime {query} ---
    var toks = ctx.curLine.split("\\s+", 3)
    var datetime = extractDatetime(toks(1))

    if (datetime.isEmpty) {      // Если поле datetime - пустое, берем из сессии
      ctx.logAcc.add(ctx.fileName, s"Bad datetime format: ${toks(1)}")
      datetime = ctx.startDatetime
    }
    val queryRow = toks(2)
    val query =
      if (queryRow.startsWith("{") && queryRow.endsWith("}")) {
        queryRow.drop(1).dropRight(1)
      }
      else {
        ctx.logAcc.add(ctx.fileName, s"Bad query format: ${ctx.curLine}")
        "unknown"
      }

    // --- id Seq[Docs] ---
    ctx.curLine = ctx.lines.next()
    toks = ctx.curLine.split("\\s+")
    val id = toks.head
    val docs = toks.tail


    QuickSearch(id, datetime, query, docs)
  }
}
