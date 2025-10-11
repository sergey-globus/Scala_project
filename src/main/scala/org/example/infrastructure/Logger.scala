package org.example.infrastructure

import scala.collection.mutable

object Logger {

  private val unknowns = mutable.ListBuffer.empty[String]
  private val unknownsSet = mutable.Set.empty[String]
  private val maxLog = 10

  // Аккумулятор: тип ошибки -> количество
  private val counts = mutable.Map.empty[String, Int]

  // Тип ошибки - все, что до ':' в сообщении, либо (если нет ':') - все сообщение
  private def errorTypeFrom(msg: String): String = {
    val idx = msg.indexOf(':')
    if (idx >= 0) msg.substring(0, idx).trim
    else if (msg != "" && msg != null) msg.trim
    else "UnknownType"
  }

  def logUnknown(fileName: String, msg: String): Unit = synchronized {
    val entry = s"$fileName | $msg"
    if (!unknownsSet.contains(entry)) {
      val et = errorTypeFrom(msg)
      counts(et) = counts.getOrElse(et, 0) + 1
      unknownsSet += entry
      if (counts(et) <= maxLog) {
        unknowns += entry
      }
    }
  }

  // Логируем исключение с stack trace
  def logException(fileName: String, ex: Throwable, context: String = ""): Unit = synchronized {
    val stack = ex.getStackTrace.take(10).map(_.toString).mkString("\n    at ")
    val msg =
      s"""$fileName | Exception${if (context.nonEmpty) s" ($context)" else ""}:
                    |Type: ${ex.getClass.getSimpleName}
                    |Message: ${ex.getMessage}
                    |Stack trace:
                    |    at $stack
                    |""".stripMargin

    unknowns += msg

    val et = ex.getClass.getSimpleName
    counts(et) = counts.getOrElse(et, 0) + 1
  }

  def getStats: String = synchronized {
    counts
      .filter { case (_, count) => count > 0 }   // только ненулевые
      .map { case (errorType, count) => s"$errorType: $count" }
      .mkString("\n")
  }

  def getUnknowns: List[String] = unknowns.toList

}
