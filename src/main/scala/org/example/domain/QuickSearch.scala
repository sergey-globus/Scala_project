package org.example.domain

import scala.collection.mutable.ListBuffer

case class QuickSearch(
                        id: String,
                        datetime: String,
                        query: String,
                        foundDocs: Seq[String]
                      ) extends Event {
  override def addToSession(session: Session): Unit =
    session.quickSearches += this
}

object QuickSearch extends EventObject[QuickSearch] {

  override protected val prefix: String = "QS"

  def parse(
             fileName: String,
             startLine: String,
             lines: Iterator[String],
             isValidDocId: String => Boolean,
             extractDateFromDatetime: String => String,
             logUnknown: (String, String) => Unit
           ): QuickSearch = {

    // --- QS datetime {query} ---
    val pattern = raw"QS\s+(\S+)\s+\{(.*)\}".r
    val (dt, query) = startLine match {
      case pattern(d, q) =>
        if (extractDateFromDatetime(d) == "invalid")
          logUnknown(fileName, s"Invalid date: $d")
        (d, q)
      case _ =>
        logUnknown(fileName, s"Bad QS line format: $startLine")
        ("unknown", "unknown")
    }

    // --- id Seq[Docs] ---
    val (id, docs) =
      if (lines.hasNext) {
        val line = lines.next().trim
        val toks = line.split("\\s+")
        if (toks.length == 1)
          logUnknown(fileName, s"[WARINING] Not founds docs inside QS: $line")
        val id = toks.headOption.getOrElse("unknown")

        val docIds = toks.tail
        val (validDocs, invalidDocs) = docIds.partition(isValidDocId)
        invalidDocs.foreach(doc => logUnknown(fileName, s"Invalid docId: $doc"))
        val docs = validDocs.toSeq

        (id, docs)
      } else ("unknown", Seq.empty[String])

    QuickSearch(id, dt, query, docs)
  }
}
