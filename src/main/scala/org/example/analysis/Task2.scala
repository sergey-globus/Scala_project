package org.example.analysis

import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.example.model.Session

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

    mergeFiles(fs, opensPath, finalOpens)

    println(s"(Task2) Each document from QS for each day -> $finalOpens")
  }

  // Вспомогательная функция для объединения файлов
  private def mergeFiles(fs: FileSystem, srcDir: Path, dstFile: Path): Unit = {
    if (!fs.exists(srcDir)) return

    val files = fs.listStatus(srcDir).filter(_.isFile).map(_.getPath)
    if (files.isEmpty) return

    if (fs.exists(dstFile)) fs.delete(dstFile, false)

    val out = fs.create(dstFile)
    try {
      files.foreach { file =>
        val in = fs.open(file)
        try {
          IOUtils.copyBytes(in, out, fs.getConf, false)
        } finally in.close()
      }
    } finally out.close()

    fs.delete(srcDir, true)
  }

}
