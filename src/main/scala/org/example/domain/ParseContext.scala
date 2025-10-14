package org.example.domain

import java.time.LocalDateTime
import scala.collection.mutable

case class ParseContext(
                         fileName: String,
                         lines: Iterator[String],
                         isValidDocId: String => Boolean,
                         extractDatetime: String => Option[LocalDateTime],
                         logUnknown: (String, String) => Unit
                       ) {
  var curLine = ""
  var startDatetime: LocalDateTime = LocalDateTime.MIN
  var endDatetime: LocalDateTime = LocalDateTime.MIN

  val cardSearches: mutable.ListBuffer[CardSearch] = mutable.ListBuffer.empty[CardSearch]
  val quickSearches: mutable.ListBuffer[QuickSearch] = mutable.ListBuffer.empty[QuickSearch]
  val docOpens: mutable.ListBuffer[DocOpen] = mutable.ListBuffer.empty[DocOpen]

  val searches: mutable.Map[String, SearchEvent] = mutable.Map.empty

  def attachToSearch(docId: String, searchId: String): Unit = {
    searches.get(searchId) match {
      case Some(searchEvent) => searchEvent.addOpenDoc(docId)
//        if (searchEvent.foundDocs.contains(docId))
//          searchEvent.addOpenDoc(docId)
//        else
//          logUnknown(fileName, s"DOC_OPEN not in foundDocs: $searchId -> $docId")
      case None              => logUnknown(fileName, s"Orphan DOC_OPEN: $searchId -> $docId")
    }
  }

  // --- Построение итоговой Session ---
  def buildSession(): Session =
    Session(
      id = fileName,
      cardSearches = cardSearches,
      quickSearches = quickSearches,
      docOpens = docOpens,
      startDatetime = startDatetime,
      endDatetime = endDatetime
    )
}
