package org.example.infrastructure

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

case class LogData(
                    counts: Map[String, Int],
                    unknowns: List[String]
                  )

class Logger extends AccumulatorV2[(String, String), LogData] {

  private val counts = mutable.Map.empty[String, Int]
  private val examples = mutable.Map.empty[String, mutable.ListBuffer[String]]
  private val maxLog = 10

  private def errorTypeFrom(msg: String): String = {
    val idx = msg.indexOf(':')
    if (idx >= 0) msg.substring(0, idx).trim
    else if (msg.nonEmpty) msg.trim
    else "UnknownType"
  }

  override def isZero: Boolean = counts.isEmpty && examples.isEmpty

  override def copy(): AccumulatorV2[(String, String), LogData] = {
    val newAcc = new Logger
    counts.foreach { case (k, v) => newAcc.counts(k) = v }
    examples.foreach { case (k, v) => newAcc.examples(k) = v.clone() }
    newAcc
  }

  override def reset(): Unit = {
    counts.clear()
    examples.clear()
  }

  override def add(v: (String, String)): Unit = synchronized {
    val (fileName, msg) = v
    val et = errorTypeFrom(msg)
    counts(et) = counts.getOrElse(et, 0) + 1
    val buf = examples.getOrElseUpdate(et, mutable.ListBuffer.empty[String])
    if (buf.size < maxLog)
      buf += s"$fileName | $msg"
  }

  def addException(fileName: String, ex: Throwable, context: String = ""): Unit = synchronized {
    val et = ex.getClass.getSimpleName
    counts(et) = counts.getOrElse(et, 0) + 1

    val buf = examples.getOrElseUpdate(et, mutable.ListBuffer.empty[String])
    if (buf.size < maxLog) {
      val stack = ex.getStackTrace.take(3).map(_.toString).mkString("\n    at ")
      val msg =
        s"""$fileName | Type: ${ex.getClass.getSimpleName}
                      |  $context
                      |  ${ex.getMessage}
                      |  Stack trace:
                      |    $stack
                      |""".stripMargin
      buf += msg
    }
  }

  override def merge(other: AccumulatorV2[(String, String), LogData]): Unit = synchronized {
    other match {
      case o: Logger =>
        o.counts.foreach { case (k, v) =>
          counts(k) = counts.getOrElse(k, 0) + v
        }

        // Мержим с ограничением maxLog на каждый тип
        o.examples.foreach { case (et, oBuf) =>
          val buf = examples.getOrElseUpdate(et, mutable.ListBuffer.empty[String])
          val remaining = maxLog - buf.size
          if (remaining > 0)
            buf ++= oBuf.take(remaining)
        }

      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass} with ${other.getClass}")
    }
  }

  override def value: LogData = {
    val combinedUnknowns =
      examples.toSeq.flatMap { case (_, msgs) => msgs }.toList
    LogData(counts.toMap, combinedUnknowns)
  }

  def getStats: String =
    counts.map { case (t, c) => s"$t: $c" }.mkString("\n")

  def getUnknowns: List[String] =
    examples.toSeq.flatMap { case (_, msgs) => msgs }.toList
}
