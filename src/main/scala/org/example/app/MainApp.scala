package org.example.app

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import java.nio.file.{Files, Paths}
import org.apache.spark.rdd.RDD
import org.example.domain.Session
import org.example.service.SessionProcessor
import org.example.infrastructure.Parser
import org.example.infrastructure.FileMerger.mergeFiles
import org.example.infrastructure.Logger

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters.asJavaIterableConverter

object MainApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Session MapReduce")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputPath = if (args.length >= 1) args(0) else "data/Сессии/*"
    val outputDir = if (args.length >= 2) args(1) else "result"

    val parsedSessions: RDD[Session] = sc.wholeTextFiles(inputPath)
      .mapPartitions { partitionIter =>
        partitionIter.map { case (filePath, content) =>
          val lines = content.split("\n").toIterator
          val fileName = Paths.get(filePath).getFileName.toString
          Parser.parseSession(fileName, lines) // парсим сессию, получаем объект Session
        }
      }

    val perSessionStatsRDD = parsedSessions.map { session =>
      SessionProcessor.processSession(session)
    }

    val totalQS = perSessionStatsRDD.map(_.qsCount).sum().toLong
    val totalCardSearch = perSessionStatsRDD.map(_.cardCount).sum().toLong
    val targetSearches = perSessionStatsRDD.map(_.TargetCardCount).sum().toLong

    println(s"(debug) QS total: $totalQS")
    println(s"(debug) CARD_SEARCH total: $totalCardSearch")
    println(s"ACC_45616 CARD_SEARCH count: $targetSearches")

    val allDocCounts = perSessionStatsRDD.flatMap(_.docOpens).reduceByKey(_ + _)


    val fs = FileSystem.get(sc.hadoopConfiguration)
    val opensPath = new Path(outputDir + "/opens")
    val finalOpens = new Path(outputDir + "/opens.txt")
    val finalLogs = new Path(outputDir + "/logs.txt")

    if (fs.exists(opensPath)) fs.delete(opensPath, true)
    if (fs.exists(finalOpens)) fs.delete(finalOpens, false)
    if (fs.exists(finalLogs)) fs.delete(finalLogs, false)

    // Сохраняем в несколько part-файлов (параллельно)
    allDocCounts
      .map { case ((date, docId), cnt) => s"$date\t$docId\t$cnt" }
      .saveAsTextFile(opensPath.toString)

    // Формируем содержимое логов
    val allLines =
      Logger.getStats.split("\n").toSeq ++
        Seq("----------") ++
        Logger.getUnknowns

    val newFinalOpens = new Path(outputDir + "/opens.txt")
    val newLogsPath = Paths.get(outputDir, "logs.txt")

    mergeFiles(fs, opensPath, newFinalOpens)
    Files.write(newLogsPath, allLines.asJava, StandardCharsets.UTF_8)

    println(s"Each document from QS for each day -> $newFinalOpens")
    println(s"Error logs -> $newLogsPath")

    spark.stop()
  }
}
