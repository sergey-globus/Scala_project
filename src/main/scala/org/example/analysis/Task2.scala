package org.example.analysis

import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, Path}
import org.example.domain.Session
import org.example.infrastructure.FileMerger

object Task2 {

  private val outputDir = "result"

  def run(sessions: RDD[Session], sc: org.apache.spark.SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val opensPath = new Path(outputDir + "/opens")
    val finalOpens = new Path(outputDir + "/opens.txt")

    if (fs.exists(opensPath)) fs.delete(opensPath, true)
    if (fs.exists(finalOpens)) fs.delete(finalOpens, false)

    val quickSearchDocCounts = sessions.flatMap { session =>
      session.quickSearches.flatMap { qs =>
        val date = qs.datetime.toLocalDate.toString
        qs.openDocs.map(docId => ((date, docId), 1))
      }
    }.reduceByKey(_ + _)

    quickSearchDocCounts
      .map { case ((date, docId), cnt) => s"$date\t$docId\t$cnt" }
      .saveAsTextFile(opensPath.toString)


    FileMerger.mergeFiles(fs, opensPath, finalOpens)

    println(s"(Task2) Each document from QS for each day -> $finalOpens")
  }

}
