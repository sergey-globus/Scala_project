package org.example.domain

import scala.collection.mutable.ListBuffer

case class CardSearch(
                       id: String,
                       datetime: String,
                       params: Seq[(Int, String)],
                       foundDocs: Seq[String]
                     ) extends Event {
  override def addToSession(session: Session): Unit =
    session.cardSearches += this
}

object CardSearch extends EventObject[CardSearch] {

  override val prefix: String = "CARD_SEARCH_START"
  override val postfix: String = "CARD_SEARCH_END"

  override def parse(
                      fileName: String,
                      startLine: String,
                      lines: Iterator[String],
                      isValidDocId: String => Boolean,
                      extractDateFromDatetime: String => String,
                      logUnknown: (String, String) => Unit
                    ): CardSearch = {

    // CARD_SEARCH_START datetime
    val toks = startLine.split("\\s+")
    val datetime = toks.length match {
      case 1 => "unknown"
      case 2 => toks(1)
      case _ =>
        logUnknown(fileName, s"Bad CARD_SEARCH_START line format: $startLine")
        toks(1)
    }

    val params = ListBuffer.empty[(Int, String)]
    var afterEnd = false

    while (lines.hasNext && !afterEnd) {
      val line = lines.next().trim

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
              logUnknown(fileName, s"Bad CARD param number: $line")
          }
        } else logUnknown(fileName, s"Bad CARD param format: $line")
      }


      // CARD_SEARCH_END
      // id Seq[Docs]
      else if (line.startsWith(postfix)) {
        afterEnd = true
      }

      else logUnknown(fileName, s"Unknown line inside CARD_SEARCH: $line")
    }

    // --- id Seq[Docs] ---
    val (id, docs) =
      if (lines.hasNext) {
        val line = lines.next().trim
        val toks = line.split("\\s+")
        if (toks.length == 1)
          logUnknown(fileName, s"[WARINING] Not founds docs inside CARD_SEARCH: $line")
        val id = toks.headOption.getOrElse("unknown")

        val docIds = toks.tail
        val (validDocs, invalidDocs) = docIds.partition(isValidDocId)
        invalidDocs.foreach(doc => logUnknown(fileName, s"Invalid docId: $doc"))
        val docs = validDocs.toSeq

        (id, docs)
      } else ("unknown", Seq.empty[String])

    CardSearch(id, datetime, params.toSeq, docs.toSeq)
  }
}
