package org.example.parser

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters.seqAsJavaListConverter
import java.nio.file.StandardOpenOption
import org.example.parser.model.{ParseContext, Session}

object RawDataProcessor {

  private val inputPath = "data/Сессии/*"
  private val outputDir = "result"

  private val logAcc = new Logger

  def process(sc: SparkContext): RDD[Session] = {
    sc.register(logAcc, "SessionLogger")

    sc.wholeTextFiles(inputPath)
      .map { case (filePath, content) =>
        val lines = content.split("\n").toIterator
        val fileName = Paths.get(filePath).getFileName.toString
        Session.parse(ParseContext(fileName, lines, logAcc))
      }.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def saveLogs(): Unit = {
    val errors = logAcc.value

    val allLines = errors.map { case (errorType, error) =>
      s"$errorType -> ${error.count}"}.toSeq ++
      Seq("----------") ++
      errors.flatMap { case (errorType, error) => error.examples.map { case (fileName, context) =>
        val msg = if (context.isEmpty) s"$errorType" else s"$errorType at " + s"$context"
        s"$fileName | $msg" } ++ Seq("")}.toSeq

    val logsPath = Paths.get(outputDir, "logs.txt")
    Files.write(
      logsPath,
      allLines.asJava,
      StandardCharsets.UTF_8,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
    println(s"Error logs -> $logsPath")
  }
}
