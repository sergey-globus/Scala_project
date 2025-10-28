package org.example.parser.model.events

import org.example.parser.model.DatetimeParser.parseDatetime
import org.example.parser.model.{EventObject, ParseContext, SearchEvent}

import java.time.LocalDateTime
import scala.collection.mutable

case class CardSearch(
                       override val id: String,
                       override val datetime: Option[LocalDateTime],
                       params: Seq[(Int, String)],
                       override val foundDocs: Seq[String]
                     ) extends SearchEvent(id, datetime, foundDocs) {

  override def addToSession(ctx: ParseContext): Unit =
    ctx.cardSearches += this
}

object CardSearch extends EventObject[CardSearch] {

  override val prefix = "CARD_SEARCH_START"
  override val postfix = "CARD_SEARCH_END"

  override def parse(ctx: ParseContext): CardSearch = {

    // CARD_SEARCH_START datetime
    var toks = ctx.curLine.split("\\s+")
    var datetime = toks.length match {
      case 2 =>
        parseDatetime(toks(1))
    }
    if (datetime.isEmpty) {     // Если поле datetime - пустое, берем из сессии
      ctx.logAcc.add("Bad datetime format in CARD_SEARCH_START", ctx.fileName, toks(1))
      datetime = ctx.startDatetime
    }

    val params = mutable.ListBuffer.empty[(Int, String)]
    ctx.curLine = ctx.lines.next()

    // $Int String
    while (ctx.curLine.startsWith("$")) {
      val toks = ctx.curLine.split("\\s+", 2)
      val num = toks(0).drop(1).toInt
      params += num -> toks(1)
      ctx.curLine = ctx.lines.next()
    }

    // CARD_SEARCH_END
    if (ctx.curLine.startsWith(postfix)) {
      ctx.curLine = ctx.lines.next()
    } else {
      ctx.logAcc.add(s"Unknown line inside CARD_SEARCH", ctx.fileName, ctx.curLine)
    }

    // --- id Seq[Docs] ---
    toks = ctx.curLine.split("\\s+")
    val id = toks.head
    val docs = toks.tail

    CardSearch(id, datetime, params.toList, docs)
  }
}
