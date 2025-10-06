package org.example.app

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.example.service.SessionProcessor
import org.example.infrastructure.SessionExtractor
import org.example.infrastructure.FileMerger.mergeFiles


object MainApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Session MapReduce")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputPath = if (args.length >= 1) args(0) else "data/Сессии/*"
    val outputDir = if (args.length >= 2) args(1) else "result"

    val fileLines: RDD[(String, String)] = sc.wholeTextFiles(inputPath)
      .flatMap { case (fileName, content) =>
        content.split("\n").map(line => (fileName, line))
      }

    val sessionLines = fileLines.mapPartitions(iter =>
      SessionExtractor.extractSessions(iter)
    )

    val sessionGrouped = sessionLines.groupByKey()
    val perSessionStatsRDD = sessionGrouped.map { case (_, sessLines) =>
      SessionProcessor.processSession(sessLines)
    }

    val totalQS = perSessionStatsRDD.map(_.qsCount).sum().toLong
    val totalCardSearch = perSessionStatsRDD.map(_.cardCount).sum().toLong
    val TargetSearches = perSessionStatsRDD.map(_.TargetCardCount).sum().toLong

    println(s"(debug) QS total: $totalQS")
    println(s"(debug) CARD_SEARCH total: $totalCardSearch")
    println(s"ACC_45616 CARD_SEARCH count: $TargetSearches")

    val allDocCounts = perSessionStatsRDD.flatMap(_.docOpens).reduceByKey(_ + _)

    import org.apache.hadoop.fs.{FileSystem, Path, FileUtil}

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val opensPath = new Path(outputDir + "/opens")
    val logsPath = new Path(outputDir + "/logs")
    val finalOpens = new Path(outputDir + "/opens.txt")
    val finalLogs = new Path(outputDir + "/logs.txt")

    if (fs.exists(opensPath)) fs.delete(opensPath, true)
    if (fs.exists(logsPath)) fs.delete(logsPath, true)
    if (fs.exists(finalOpens)) fs.delete(finalOpens, false)
    if (fs.exists(finalLogs)) fs.delete(finalLogs, false)
    if (!fs.exists(logsPath.getParent)) fs.mkdirs(logsPath.getParent)

    // Сохраняем в несколько part-файлов (параллельно)
    allDocCounts
      .map { case ((date, docId), cnt) => s"$date\t$docId\t$cnt" }
      .saveAsTextFile(opensPath.toString)

    val unknownsRDD = sc.parallelize(SessionProcessor.getUnknowns)
    unknownsRDD.saveAsTextFile(logsPath.toString)

    val newFinalOpens = new Path(outputDir + "/opens.txt")
    val newLogsPath = new Path(outputDir + "/logs.txt")

    mergeFiles(fs, opensPath, newFinalOpens)
    mergeFiles(fs, logsPath, newLogsPath)

    println(s"Each document found through QS was opened each day -> $newFinalOpens")
    println(s"Error logs -> $newLogsPath")

    spark.stop()
  }
}
