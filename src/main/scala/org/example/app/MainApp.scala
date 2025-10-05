package org.example.app

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.example.service.SessionProcessor

object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Session MapReduce")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputPath = if (args.length >= 1) args(0) else "data/Сессии/*"
    val outputDir = if (args.length >= 2) args(1) else "result"

    // wholeTextFiles дает (filename, content)
    val fileLines: RDD[(String, String)] = sc.wholeTextFiles(inputPath)
      .flatMap { case (fileName, content) =>
        content.split("\n").map(line => (fileName, line))
      }

    // Сессии
    val sessionStartPrefix = "SESSION_START"
    val sessionEndPrefix = "SESSION_END"

    val sessionLines = fileLines.mapPartitions(iter => {
      import scala.collection.mutable.ListBuffer
      var currentSession: Option[String] = None
      val out = ListBuffer.empty[(String, (String, String))] // (sessionId, (fileName, line))
      while (iter.hasNext) {
        val (fileName, lineRaw) = iter.next()
        val line = Option(lineRaw).getOrElse("").trim
        if (line.startsWith(sessionStartPrefix)) {
          val parts = line.split("\\s+", 2)
          currentSession = Some(if (parts.length >= 2 && parts(1).nonEmpty) parts(1)
          else "sess_unknown_" + java.util.UUID.randomUUID())
        } else if (line.startsWith(sessionEndPrefix)) {
          currentSession = None
        } else {
          currentSession.foreach(id => out += ((id, (fileName, line))))
        }
      }
      out.iterator
    })

    val sessionGrouped = sessionLines.groupByKey()

    val perSessionStatsRDD = sessionGrouped.map { case (_, sessLines) =>
      SessionProcessor.processSession(sessLines)
    }

    val totalQS = perSessionStatsRDD.map(_.qsCount.toLong).sum()
    val totalCardSearch = perSessionStatsRDD.map(_.cardCount.toLong).sum()
    val acc45616Searches = perSessionStatsRDD.map(_.acc45616CardCount.toLong).sum()
    println(s"QS total: $totalQS")
    println(s"CARD_SEARCH total: $totalCardSearch")
    println(s"ACC_45616 CARD_SEARCH count: $acc45616Searches")

    val allDocCounts = perSessionStatsRDD.flatMap(_.docOpens).reduceByKey(_ + _)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val opensPath = new Path(outputDir + "/opens.txt")
    val logsPath = new Path(outputDir + "/logs/unknown.log")
    if (fs.exists(opensPath)) fs.delete(opensPath, true)
    if (fs.exists(logsPath)) fs.delete(logsPath, true)
    if (!fs.exists(logsPath.getParent)) fs.mkdirs(logsPath.getParent)

    // Сохраняем opens.txt
    allDocCounts.map { case ((date, docId), cnt) => s"$date\t$docId\t$cnt" }
      .coalesce(1)
      .saveAsTextFile(opensPath.toString)

    // Сохраняем unknown.log
    val unknownsRDD = sc.parallelize(SessionProcessor.getUnknowns)
    unknownsRDD.coalesce(1).saveAsTextFile(logsPath.toString)

    spark.stop()
  }
}
