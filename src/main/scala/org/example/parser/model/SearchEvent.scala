package org.example.parser.model

import java.time.LocalDateTime
import scala.collection.mutable

abstract class SearchEvent(
                            val id: String,
                            override val datetime: Option[LocalDateTime],
                            val foundDocs: Seq[String]
                          ) extends Event(datetime) {

  val docOpens: mutable.ListBuffer[(Option[LocalDateTime], String)] =
    mutable.ListBuffer.empty

  private[model] def addDocOpen(dateTime: Option[LocalDateTime], docId: String): Unit =
    docOpens += dateTime -> docId

  final def getDocOpens: Seq[(Option[LocalDateTime], String)] = docOpens

  private def addToSearches(ctx: ParseContext): Unit = ctx.searches += (id -> this)

  override def addToContext(ctx: ParseContext): Unit = {
    super.addToContext(ctx)
    addToSearches(ctx)
  }
}
