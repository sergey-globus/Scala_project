package org.example.domain

import scala.collection.mutable.ListBuffer

case class DocOpen(
                    datetime: String,
                    searchId: String,
                    docId: String
                  ) extends Event {
  override def addToSession(session: Session): Unit =
    session.docOpens += this
}

object DocOpen extends EventObject[DocOpen] {

  override val prefix: String = "DOC_OPEN"

  def parse(
             fileName: String,
             startLine: String,
             lines: Iterator[String],
             isValidDocId: String => Boolean,
             extractDateFromDatetime: String => String,
             logUnknown: (String, String) => Unit
           ): DocOpen = {

    // DOC_OPEN [datetime] searchId docId
    val toks = startLine.split("\\s+")
    val (dtCandidate, searchId, docCandidate) = toks.length match {
      case 4 => (toks(1), toks(2), toks(3))   // формат с датой
      case 3 => ("unknown", toks(1), toks(2))  // формат без даты
      case _ =>
        logUnknown(fileName, s"Bad DOC_OPEN line format: $startLine")
        ("unknown", "unknown", "unknown")
    }

    val dt = extractDateFromDatetime(dtCandidate)
    val docId = if (isValidDocId(docCandidate)) docCandidate else "unknown"
    if (dt == "invalid") logUnknown(fileName, s"Invalid date: $dtCandidate")
    if (docId == "unknown") logUnknown(fileName, s"Unknown document id: $docCandidate")

    DocOpen(dt, searchId, docId)
  }
}
