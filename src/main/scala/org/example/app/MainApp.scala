package org.example.app

import org.apache.spark.sql.SparkSession
import org.example.parser.RawDataProcessor
import org.example.analysis.{Task1, Task2, ForDebug}

object MainApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sessions")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    // Парсим сессии
    val sessions = RawDataProcessor.process(sc)

    // Выполняем задачи
    ForDebug.run(sessions)
    Task1.run(sessions)
    Task2.run(sessions, sc)

    RawDataProcessor.saveLogs()

    spark.stop()
  }

}
