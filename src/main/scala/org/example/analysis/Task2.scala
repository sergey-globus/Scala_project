package org.example.analysis

import org.example.services.SparkUtils.mergeFiles

import org.apache.spark.rdd.RDD
import org.example.parser.model.Session

/**
 * Количество открытий каждого документа, найденного через быстрый поиск за каждый день.
 * Результат сохраняется в CSV файл с форматом:
 *   date,docId,count
 */
object Task2 {

  private val opensFile = "result/opens"

  def run(sessions: RDD[Session], sc: org.apache.spark.SparkContext): Unit = {

    sessions.flatMap { session =>
      session.quickSearches.flatMap { qs =>
        qs.docOpens.map { case (datetime, docId) =>
          val date = datetime.map(_.toLocalDate.toString).getOrElse("None")
          ((date, docId), 1)
        }.toSet   // убираем дубликаты в рамках одного qs
      }
    }
    .reduceByKey(_ + _)
    .map { case ((date, docId), cnt) => s"$date,$docId,$cnt" }
    .saveAsTextFile(opensFile)

    mergeFiles(sc, opensFile)

    println(s"(Task2) Each document from QS for each day -> $opensFile.csv")
  }

}
