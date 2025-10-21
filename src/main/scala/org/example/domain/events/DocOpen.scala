package org.example.domain.events

import org.example.domain.{Event, EventObject, ParseContext}

import java.time.LocalDateTime

case class DocOpen(
                    datetime: LocalDateTime,
                    searchId: String,
                    docId: String
                  ) extends Event {
  require(datetime != null, "Not found DocOpen.datetime")
  require(searchId.nonEmpty, "Not found DocOpen.searchId")
  require(docId.nonEmpty, "Not found DocOpen.docId")

  override def addToSession(ctx: ParseContext): Unit =
    ctx.docOpens += this
}

object DocOpen extends EventObject[DocOpen] {

  override val prefix = "DOC_OPEN"

  def parse(ctx: ParseContext): DocOpen = {

    // DOC_OPEN [datetime] searchId docId
    val toks = ctx.curLine.split("\\s+")
    val (dt, searchId, docId) = toks.length match {
      case 4 =>      // формат с датой
        val dtCandidate = ctx.extractDatetime(toks(1))
        val searchIdCandidate = toks(2)
        val docCandidate = toks(3)
        if (!ctx.isValidDocId(docCandidate))
          ctx.logUnknown(ctx.fileName, s"[WARNING] Invalid DocId: $docCandidate")
        (dtCandidate, searchIdCandidate, docCandidate)
      case 3 =>     // формат без даты
        val searchIdCandidate = toks(1)
        val dtCandidate = ctx.datetimeFromSearch(searchIdCandidate)
        val docCandidate = toks(2)
        if (!ctx.isValidDocId(docCandidate))
          ctx.logUnknown(ctx.fileName, s"[WARNING] Invalid DocId: $docCandidate")
        (dtCandidate, searchIdCandidate, docCandidate)
    }

    val docOpen = DocOpen(dt, searchId, docId)

    ctx.attachToSearch(docId, searchId)
    docOpen.addToSession(ctx)
    docOpen
  }
}
