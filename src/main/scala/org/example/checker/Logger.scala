package org.example.checker

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

case class LogEntry(count: Int, examples: Vector[(String, String)])

// IN: (fileName, msg), OUT: Map[errorType, LogEntry <-- (count, (fileName, msg))]
class Logger(val maxLog: Int = 10) extends AccumulatorV2[(String, String), Map[String, LogEntry]] {

  private val data = mutable.Map.empty[String, LogEntry]

  private def errorTypeFrom(msg: String): String = {
    val idx = msg.indexOf(':')
    if (idx >= 0) msg.substring(0, idx).trim
    else if (msg.nonEmpty) msg.trim
    else "UnknownType"
  }

  override def isZero: Boolean = data.isEmpty

  override def copy(): AccumulatorV2[(String, String), Map[String, LogEntry]] = {
    val newAcc = new Logger(maxLog)
    data.foreach { case (k, v) =>
      newAcc.data(k) = v.copy()
    }
    newAcc
  }

  override def reset(): Unit = data.clear()

  override def add(v: (String, String)): Unit = synchronized {
    val (fileName, msg) = v
    val et = errorTypeFrom(msg)
    val old = data.getOrElse(et, LogEntry(0, Vector.empty))
    val newExamples =
      if (old.examples.size < maxLog) old.examples :+ (fileName -> msg)
      else old.examples
    data(et) = old.copy(count = old.count + 1, examples = newExamples)
  }

  def addException(fileName: String, ex: Throwable, context: String = ""): Unit = synchronized {
    val et = ex.getClass.getSimpleName +
      " in " +
      ex.getStackTrace.headOption.map(_.toString).getOrElse("")

    val old = data.getOrElse(et, LogEntry(0, Vector.empty))
    val stack = ex.getStackTrace.take(3).map(_.toString).mkString("\n    at ")
    val msg =
      s"""Type: ${ex.getClass.getSimpleName}
          |  $context
          |  ${ex.getMessage}
          |  Stack trace:
          |    $stack""".stripMargin
    val newExamples =
      if (old.examples.size < maxLog) old.examples :+ (fileName -> msg)
      else old.examples
    data(et) = old.copy(count = old.count + 1, examples = newExamples)
  }

  override def merge(other: AccumulatorV2[(String, String), Map[String, LogEntry]]): Unit = synchronized {
    other match {
      case o: Logger =>
        o.data.foreach { case (et, logEntry) =>
          val old = data.getOrElse(et, LogEntry(0, Vector.empty))
          val remaining = maxLog - old.examples.size
          val mergedExamples = old.examples ++ logEntry.examples.take(remaining)
          data(et) = old.copy(count = old.count + logEntry.count, examples = mergedExamples)
        }
    }
  }

  override def value: Map[String, LogEntry] = data.toMap
}
