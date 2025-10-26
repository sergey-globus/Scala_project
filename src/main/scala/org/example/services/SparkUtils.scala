package org.example.services

import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedWriter, OutputStreamWriter}

object SparkUtils {

  def mergeFiles(sc: org.apache.spark.SparkContext, srcDir: String): Unit = {
    val srcPath = new Path(srcDir)

    val fs = FileSystem.get(sc.hadoopConfiguration)

    if (!fs.exists(srcPath)) return

    val files = fs.listStatus(srcPath)
      .filter(_.isFile)
      .map(_.getPath)
      .filter { p =>
        val name = p.getName
        !name.startsWith("_") && !name.startsWith(".")
      }

    if (files.isEmpty) return

    val dstFile = new Path(srcPath.getParent, srcPath.getName + ".csv")

    if (fs.exists(dstFile)) fs.delete(dstFile, false)

    val outStream = fs.create(dstFile)
    val writer = new BufferedWriter(new OutputStreamWriter(outStream, "UTF-8"))

    try {
      files.foreach { file =>
        val in = fs.open(file)
        try {
          scala.io.Source.fromInputStream(in, "UTF-8").getLines().foreach { line =>
            writer.write(line)
            writer.newLine()
          }
        } finally in.close()
      }
    } finally {
      writer.close()
    }

    fs.delete(srcPath, true)
  }
}
