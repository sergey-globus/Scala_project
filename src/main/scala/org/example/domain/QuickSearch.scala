package org.example.domain

import java.time.LocalDateTime
import scala.collection.mutable

case class QuickSearch(
                        override val id: String,
                        datetime: LocalDateTime,
                        query: String,
                        override val foundDocs: Seq[String]
                      ) extends SearchEvent(id, foundDocs) {

  override def addToSession(ctx: ParseContext): Unit =
    ctx.quickSearches += this
}

object QuickSearch extends EventObject[QuickSearch] {

  override protected val prefix: String = "QS"

  def parse(ctx: ParseContext): QuickSearch = {


    // --- QS datetime {query} ---
    val toks = ctx.curLine.split("\\s+", 3)
    val (dt, query) =
      if (toks.length < 3 || toks(0) != "QS") {
        ctx.logUnknown(ctx.fileName, s"Bad QS line format: ${ctx.curLine}")
        (LocalDateTime.MIN, "unknown")
      } else {
        val dtCandidate = ctx.extractDatetime(toks(1)).getOrElse {
          ctx.logUnknown(ctx.fileName, s"Invalid date: ${toks(1)}")
          LocalDateTime.MIN
        }
        val queryRow = toks(2)
        val queryCandidate =
          if (queryRow.startsWith("{") && queryRow.endsWith("}"))
            queryRow.drop(1).dropRight(1)
          else {
            ctx.logUnknown(ctx.fileName, s"Bad query format: ${ctx.curLine}")
            "unknown"
          }
        (dtCandidate, queryCandidate)
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

    val qs = QuickSearch(id, dt, query, docs)
    qs.addToSearches(ctx)
    qs
  }
}
