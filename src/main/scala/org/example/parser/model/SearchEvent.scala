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

  private[model] def addDocOpen(dateTime: Option[LocalDateTime], docId: String): Unit = {
    docOpens += dateTime -> docId
  }

  final def getDocOpens: Seq[(Option[LocalDateTime], String)] = docOpens
}

trait SearchEventObject[T <: SearchEvent] extends EventObject[T] {

  private def addToSearches(ctx: ParseContext, event: T): Unit = {
    ctx.searches += (event.id -> event)
  }

  override def addToContext(ctx: ParseContext, event: T): Unit = {
    super.addToContext(ctx, event)
    addToSearches(ctx, event)
  }
}
