package org.example.app

import org.apache.spark.sql.SparkSession
import org.example.service.Service
import org.apache.hadoop.fs.{FileSystem, Path}

object SessionRDDApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Session RDD App")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val inputPath = if (args.length >= 1) args(0) else "data/Сессии/*"
    val outputDir = if (args.length >= 2) args(1) else "result"

    val sessions = Service.parseSessions(sc, inputPath)
    val aggregated = Service.aggregateResults(sessions)

    // Метасчётчики
    val metaCounts = aggregated.filter { case ((date, _), _) => date == "__META__" }.collect().toMap
    println(s"(Отладка) Всего быстрых поисков (QS): ${metaCounts.getOrElse(("__META__", "QS_COUNT"), 0)}")
    println(s"(Отладка) Всего карточных поисков (CARD_SEARCH): ${metaCounts.getOrElse(("__META__", "CARD_COUNT"), 0)}")
    println(s"Количество раз, когда в карточке производили поиск документа ACC_45616: ${metaCounts.getOrElse(("__META__", "ACC_45616_CARD_SEARCH"), 0)}")

    val realCounts = aggregated.filter { case ((date, _), _) => date != "__META__" }

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = new Path(outputDir)
    if (fs.exists(outputPath)) fs.delete(outputPath, true)

    realCounts.map { case ((date, docId), cnt) => s"$date\t$docId\t$cnt" }
      .coalesce(1)
      .saveAsTextFile(outputDir)

    // Переименование part- файла
    try {
      val files = fs.listStatus(outputPath).map(_.getPath.getName).filter(_.startsWith("part-"))
      if (files.nonEmpty) {
        val src = new Path(outputDir + "/" + files.head)
        val dst = new Path(outputDir + "/opens.txt")
        if (fs.exists(dst)) fs.delete(dst, true)
        fs.rename(src, dst)
      }
    } catch {
      case e: Exception => println(s"Не удалось переименовать part-файл: ${e.getMessage}")
    }

    spark.stop()
  }
}
