package org.example.domain

import java.time.LocalDateTime
import scala.collection.mutable

case class DocOpen(
                    datetime: Option[LocalDateTime],
                    searchId: String,
                    docId: String
                  ) extends Event {

  override def addToSession(ctx: ParseContext): Unit =
    ctx.docOpens += this
}

object DocOpen extends EventObject[DocOpen] {

  override val prefix: String = "DOC_OPEN"

  def parse(ctx: ParseContext): DocOpen = {

    // DOC_OPEN [datetime] searchId docId
    val toks = ctx.curLine.split("\\s+")
    val (dt, searchId, docId) = toks.length match {
      case 4 =>      // формат с датой
        val dtCandidate = ctx.extractDatetime(toks(1)).getOrElse {
          ctx.logUnknown(ctx.fileName, s"Invalid date: ${toks(1)}")
          LocalDateTime.MIN
        }
        val docCandidate = if (ctx.isValidDocId(toks(3))) toks(3) else {
          ctx.logUnknown(ctx.fileName, s"Unknown document id: ${toks(3)}")
          "unknown"
        }
        (Some(dtCandidate), toks(2), docCandidate)
      case 3 =>     // формат без даты
        val docCandidate = if (ctx.isValidDocId(toks(2))) toks(2) else {
          ctx.logUnknown(ctx.fileName, s"Unknown document id: ${toks(2)}")
          "unknown"
        }
        (None, toks(1), docCandidate)
      case _ =>
        ctx.logUnknown(ctx.fileName, s"Bad DOC_OPEN line format: ${ctx.curLine}")
        (Some(LocalDateTime.MIN), "unknown", "unknown")
    }

    ctx.attachToSearch(docId, searchId)

    DocOpen(dt, searchId, docId)
  }
}
