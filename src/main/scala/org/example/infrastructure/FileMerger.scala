package org.example.infrastructure

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object FileMerger {
  // Объединяет все файлы из srcDir в один dstFile и удаляет исходную директорию */
  def mergeFiles(fs: FileSystem, srcDir: Path, dstFile: Path): Unit = {
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
