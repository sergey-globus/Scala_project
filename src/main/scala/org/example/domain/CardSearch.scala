package org.example.domain

import java.time.LocalDateTime
import scala.collection.mutable

case class CardSearch(
                       override val id: String,
                       datetime: LocalDateTime,
                       params: Seq[(Int, String)],
                       override val foundDocs: Seq[String]
                     ) extends SearchEvent(id, foundDocs) {

  override def addToSession(ctx: ParseContext): Unit =
    ctx.cardSearches += this
}

object CardSearch extends EventObject[CardSearch] {

  override val prefix: String = "CARD_SEARCH_START"
  override val postfix: String = "CARD_SEARCH_END"

  override def parse(ctx: ParseContext): CardSearch = {

    // CARD_SEARCH_START datetime
    val toks = ctx.curLine.split("\\s+")
    val datetime: LocalDateTime = toks.length match {
      case 2 =>
        ctx.extractDatetime(toks(1)).getOrElse {
          ctx.logUnknown(ctx.fileName, s"Invalid date: ${toks(1)}")
          LocalDateTime.MIN
        }
      case _ =>
        ctx.logUnknown(ctx.fileName, s"Bad CARD_SEARCH_START line format: ${ctx.curLine}")
        LocalDateTime.MIN
    }

    val params = mutable.ListBuffer.empty[(Int, String)]
    var afterEnd = false

    while (ctx.lines.hasNext && !afterEnd) {
      val line = ctx.lines.next().trim

      if (line.isEmpty) {}

      // $Int String
      else if (line.startsWith("$")) {
        val toks = line.split("\\s+", 2)
        if (toks.length == 2) {
          try {
            val num = toks(0).drop(1).toInt
            params += num -> toks(1)
          } catch {
            case _: NumberFormatException =>
              ctx.logUnknown(ctx.fileName, s"Bad CARD param number: $line")
          }
        } else ctx.logUnknown(ctx.fileName, s"Bad CARD param format: $line")
      }

      // CARD_SEARCH_END
      else if (line.startsWith(postfix)) {
        afterEnd = true
      }

      else ctx.logUnknown(ctx.fileName, s"Unknown line inside CARD_SEARCH: $line")
    }

    // --- id Seq[Docs] ---
    val (id, docs) =
      if (ctx.lines.hasNext) {
        val line = ctx.lines.next().trim
        val toks = line.split("\\s+")
        val id = toks.headOption.getOrElse("unknown")

        val docIds = toks.tail
        val (validDocs, invalidDocs) = docIds.partition(ctx.isValidDocId)
        invalidDocs.foreach(doc => ctx.logUnknown(ctx.fileName, s"Invalid docId: $doc"))
        val docs = validDocs.toSeq

        (id, docs)
      } else ("unknown", Seq.empty[String])

    val cs = CardSearch(id, datetime, params, docs)
    cs.addToSearches(ctx)
    cs
  }
}
