package org.example.parser.model

import org.example.parser.Logger
import org.example.parser.model.events.{CardSearch, DocOpen, QuickSearch}

import java.time.LocalDateTime
import scala.collection.mutable

case class ParseContext(fileName: String, lines: Iterator[String], logAcc: Logger) {

  var curLine = ""
  var startDatetime: Option[LocalDateTime] = None
  var endDatetime: Option[LocalDateTime] = None

  val cardSearches: mutable.ListBuffer[CardSearch] = mutable.ListBuffer.empty[CardSearch]
  val quickSearches: mutable.ListBuffer[QuickSearch] = mutable.ListBuffer.empty[QuickSearch]
  val docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty[DocOpen]

  val searches: mutable.Map[String, SearchEvent] = mutable.Map.empty

  def +=(event: Event): Unit = event.addToContext(this)

  def attachDocOpenToSearch(dt: Option[LocalDateTime], docId: String, searchId: String): Unit = {
    searches.get(searchId) match {
      case Some(searchEvent) =>
        // Если поле datetime в DOC_OPEN - пустое, берем из searchEvent
        val datetime = if (dt.isEmpty) searchEvent.datetime else dt
        searchEvent.addDocOpen(datetime, docId)
//        if (searchEvent.foundDocs.contains(docId))
//          searchEvent.addOpenDoc(docId)
//        else
//          logUnknown(fileName, s"DOC_OPEN not in foundDocs: $searchId -> $docId")
      case None => logAcc.add(s"[WARNING] Orphan DOC_OPEN", fileName, curLine)
    }
  }


  def buildSession(): Session =
    Session(
      id = fileName,
      cardSearches = cardSearches.toList,
      quickSearches = quickSearches.toList,
      docOpens = docOpens.toList,
      startDatetime = startDatetime,
      endDatetime = endDatetime
    )
}
