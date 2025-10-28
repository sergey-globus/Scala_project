package org.example.parser

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

case class LogEntry(count: Int, examples: Vector[(String, String)]) {

  def addLog(example: (String, String), maxLog: Int): LogEntry = {
    val newExamples =
      if (examples.size < maxLog) examples :+ example
      else examples
    copy(count = count + 1, examples = newExamples)
  }
}

// IN: (typeError, fileName, context)
// OUT: Map[errorType, LogEntry <-- (count, (fileName, context))]
class Logger(val maxLog: Int = 10)
  extends AccumulatorV2[(String, String, String), Map[String, LogEntry]] {

  private val data = mutable.Map.empty[String, LogEntry]

  override def isZero: Boolean = data.isEmpty

  override def copy(): AccumulatorV2[(String, String, String), Map[String, LogEntry]] = {
    val newAcc = new Logger(maxLog)
    data.foreach { case (k, v) => newAcc.data(k) = v.copy() }
    newAcc
  }

  override def reset(): Unit = data.clear()

  override def add(v: (String, String, String)): Unit = synchronized {
    val (errorType, fileName, context) = v
    val msg = context.trim
    val old = data.getOrElse(errorType, LogEntry(0, Vector.empty))
    data(errorType) = old.addLog(fileName -> msg, maxLog)
  }

  def addException(ex: Throwable, fileName: String, context: String): Unit = {
    val errorType = ex.getClass.getSimpleName +
      " in " +
      ex.getStackTrace.headOption.map(_.toString).getOrElse("")
    val stack = ex.getStackTrace.slice(1, 3).map(_.toString).mkString(" at\n  ")
    val ctx =
      s"""'
         |  $stack
         |  Message: ${ex.getMessage}
         |  $context
         |""".stripMargin

    add((errorType, fileName, ctx))
  }

  override def merge(other: AccumulatorV2[(String, String, String), Map[String, LogEntry]]): Unit = synchronized {
    other match {
      case o: Logger =>
        o.data.foreach { case (errorType, logEntry) =>
          val old = data.getOrElse(errorType, LogEntry(0, Vector.empty))
          val remaining = maxLog - old.examples.size
          val mergedExamples = old.examples ++ logEntry.examples.take(remaining)
          data(errorType) = old.copy(count = old.count + logEntry.count, examples = mergedExamples)
        }
    }
  }

  override def value: Map[String, LogEntry] = data.toMap
}
