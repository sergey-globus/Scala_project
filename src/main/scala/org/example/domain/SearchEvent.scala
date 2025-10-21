package org.example.domain

import java.time.LocalDateTime
import scala.collection.mutable

abstract class SearchEvent(
                         val id: String,
                         val datetime: LocalDateTime,
                         val foundDocs: Seq[String]
                       ) extends Event {

  val openDocs: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]

  def addOpenDoc(docId: String): Unit = openDocs += docId

  def getOpenDocs: mutable.ListBuffer[String] = openDocs

  // Добавление пары (SearchId, SearchEvent) в контекст
  def addToSearches(ctx: ParseContext): Unit = ctx.searches += (id -> this)
}
