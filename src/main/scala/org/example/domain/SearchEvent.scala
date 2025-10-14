package org.example.domain

import scala.collection.mutable

abstract class SearchEvent(
                         val id: String,
                         val foundDocs: Seq[String]
                       ) extends Event {

  protected val openDocs: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]

  def addOpenDoc(docId: String): Unit = openDocs += docId

  def getOpenDocs: mutable.ListBuffer[String] = openDocs

  // Добавление пары (SearchId, SearchEvent) в контекст
  def addToSearches(ctx: ParseContext): Unit = ctx.searches += (id -> this)
}
