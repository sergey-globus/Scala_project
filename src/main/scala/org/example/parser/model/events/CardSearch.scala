package org.example.parser.model.events

import org.example.checker.Validator.extractDatetime
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
        extractDatetime(toks(1))
    }
    //
    if (datetime.isEmpty) {     // Если поле datetime - пустое, берем из сессии
      ctx.logAcc.add(ctx.fileName, s"Bad datetime format: ${toks(1)}")
      datetime = ctx.startDatetime
    }

    val params = mutable.ListBuffer.empty[(Int, String)]
    var afterEnd = false

    while (ctx.lines.hasNext && !afterEnd) {
      ctx.curLine = ctx.lines.next()

      // $Int String
      if (ctx.curLine.startsWith("$")) {
        val toks = ctx.curLine.split("\\s+", 2)
        val num = toks(0).drop(1).toInt
        params += num -> toks(1)
      }

      // CARD_SEARCH_END
      else if (ctx.curLine.startsWith(postfix)) {
        afterEnd = true
      }

      else ctx.logAcc.add(ctx.fileName, s"Unknown line inside CARD_SEARCH: ${ctx.curLine}")
    }

    // --- id Seq[Docs] ---
    ctx.curLine = ctx.lines.next()
    toks = ctx.curLine.split("\\s+")
    val id = toks.head
    val docs = toks.tail


    CardSearch(id, datetime, params.toList, docs)
  }
}
