package org.example.parser

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters.seqAsJavaListConverter
import java.nio.file.StandardOpenOption
import org.example.domain.Session
import org.example.infrastructure.{Logger, Parser}

object RawDataProcessor {

  private val inputPath = "data/Сессии/*"
  private val outputDir = "result"

  private val logAcc = new Logger

  // Парсинг сессий и регистрация аккумулятора
  def process(sc: SparkContext): RDD[Session] = {
    sc.register(logAcc, "SessionLogger")

    val rawRDD: RDD[(String, String)] = sc.wholeTextFiles(inputPath)

    val sessionsRDD: RDD[Session] = rawRDD.mapPartitions { partitionIter =>
      partitionIter.map { case (filePath, content) =>
        val lines = content.split("\n").toIterator
        val fileName = Paths.get(filePath).getFileName.toString
        Parser.parseSession(fileName, lines, logAcc)
      }
    }.persist(StorageLevel.MEMORY_AND_DISK)

    sessionsRDD
  }

  def saveLogs(): Unit = {
    val logs = logAcc.value
    val allLines = logs.counts.map { case (k, v) => s"$k -> $v" }.toSeq ++
      Seq("----------") ++ logs.unknowns
    val logsPath = Paths.get(outputDir, "logs.txt")
    Files.write(logsPath, allLines.asJava, StandardCharsets.UTF_8,
      StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    println(s"Error logs -> $logsPath")
  }

}
