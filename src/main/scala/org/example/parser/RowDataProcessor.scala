package org.example.parser

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters.seqAsJavaListConverter
import java.nio.file.StandardOpenOption

import org.example.domain.Session
import org.example.infrastructure.{Logger, Parser, LogData}

object RowDataProcessor {

  private val inputPath: String = "data/Сессии/*"
  private val outputDir: String = "result"

  // После action сохраняем значение аккумулятора
  private var logAccValue: Option[LogData] = None

  /** Парсинг сессий и регистрация Logger */
  def process(sc: SparkContext): RDD[Session] = {
    val acc = new Logger
    sc.register(acc, "SessionLogger")

    val rawRDD: RDD[(String, String)] = sc.wholeTextFiles(inputPath)

    val sessionsRDD: RDD[Session] = rawRDD.mapPartitions { partitionIter =>
      partitionIter.map { case (filePath, content) =>
        val lines = content.split("\n").toIterator
        val fileName = Paths.get(filePath).getFileName.toString
        Parser.parseSession(fileName, lines, acc)
      }
    }

    // Легкий action (для логов), чтобы spark не откладывал вычисления
    sessionsRDD.count()

    logAccValue = Some(acc.value)

    sessionsRDD
  }

  def saveLogs(): Unit = {
    logAccValue match {
      case Some(value) =>
        val allLines = value.counts.map { case (k, v) => s"$k -> $v" }.toSeq ++
          Seq("----------") ++ value.unknowns
        val logsPath = Paths.get(outputDir, "logs.txt")
        Files.write(logsPath, allLines.asJava, StandardCharsets.UTF_8,
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        println(s"Error logs -> $logsPath")
      case None =>
        println("Аккумулятор не инициализирован. Сначала вызовите process.")
    }
  }

}
