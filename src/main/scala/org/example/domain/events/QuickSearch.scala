package org.example.domain.events

import org.example.domain.{EventObject, ParseContext, SearchEvent}

import java.time.LocalDateTime

case class QuickSearch(
                        override val id: String,
                        override val datetime: LocalDateTime,
                        query: String,
                        override val foundDocs: Seq[String]
                      ) extends SearchEvent(id, datetime, foundDocs) {
  require(id.nonEmpty, "Not found QuickSearch.id")
  require(datetime != null, "Not found QuickSearch.datetime")
  require(query.nonEmpty, "Not found QuickSearch.query")

  override def addToSession(ctx: ParseContext): Unit =
    ctx.quickSearches += this
}

object QuickSearch extends EventObject[QuickSearch] {

  override protected val prefix = "QS"

  def parse(ctx: ParseContext): QuickSearch = {

    // --- QS datetime {query} ---
    var toks = ctx.curLine.split("\\s+", 3)
    val dt = ctx.extractDatetime(toks(1))
    val queryRow = toks(2)
    val query =
      if (queryRow.startsWith("{") && queryRow.endsWith("}"))
        queryRow.drop(1).dropRight(1)
      else {
        ctx.logUnknown(ctx.fileName, s"Bad query format: ${ctx.curLine}")
        "unknown"
      }

    // --- id Seq[Docs] ---
    ctx.curLine = ctx.lines.next()
    toks = ctx.curLine.split("\\s+")
    val id = toks.head
    val docs = toks.tail
    docs.foreach(doc => if (!ctx.isValidDocId(doc))
      ctx.logUnknown(ctx.fileName, s"[WARNING] Invalid DocId: $doc"))


    val qs = QuickSearch(id, dt, query, docs)

    qs.addToSearches(ctx)
    qs.addToSession(ctx)
    qs
  }
}
