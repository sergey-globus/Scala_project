package org.example.domain.events

import org.example.domain.{EventObject, ParseContext, SearchEvent}

import java.time.LocalDateTime
import scala.collection.mutable

case class CardSearch(
                       override val id: String,
                       override val datetime: LocalDateTime,
                       params: Seq[(Int, String)],
                       override val foundDocs: Seq[String]
                     ) extends SearchEvent(id, datetime, foundDocs) {
  require(id.nonEmpty, "Not found CardSearch.id")
  require(datetime != null, "Not found CardSearch.datetime")
  require(params.nonEmpty, "Not found CardSearch.params")

  override def addToSession(ctx: ParseContext): Unit =
    ctx.cardSearches += this
}

object CardSearch extends EventObject[CardSearch] {

  override val prefix = "CARD_SEARCH_START"
  override val postfix = "CARD_SEARCH_END"

  override def parse(ctx: ParseContext): CardSearch = {

    // CARD_SEARCH_START datetime
    var toks = ctx.curLine.split("\\s+")
    val datetime: LocalDateTime = toks.length match {
      case 2 =>
        ctx.extractDatetime(toks(1))
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

      else ctx.logUnknown(ctx.fileName, s"Unknown line inside CARD_SEARCH: ${ctx.curLine}")
    }

    // --- id Seq[Docs] ---
    ctx.curLine = ctx.lines.next()
    toks = ctx.curLine.split("\\s+")
    val id = toks.head
    val docs = toks.tail
    docs.foreach(doc => if (!ctx.isValidDocId(doc))
      ctx.logUnknown(ctx.fileName, s"[WARNING] Invalid DocId: $doc"))


    val cs = CardSearch(id, datetime, params, docs)

    cs.addToSearches(ctx)
    cs.addToSession(ctx)
    cs
  }
}
