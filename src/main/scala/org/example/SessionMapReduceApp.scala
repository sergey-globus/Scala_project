package org.example

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Date, Locale}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object SessionMapReduceApp {

  // Безопасно разбивает строку по пробелам, можно указать лимит
  private def safeSplit(s: String, limit: Int = -1): Array[String] =
    if (s == null) Array.empty else s.split("\\s+", limit).map(_.trim)

  // Нормализует docId в верхний регистр, обрезая лишние пробелы
  private def norm(s: String): String =
    if (s == null) "" else s.trim.toUpperCase

  // Проверяет, похожа ли строка на timestamp в известных форматах
  private def isLikelyTimestamp(s: String): Boolean = {
    if (s == null || s.isEmpty) return false
    // Проверяем форматы типа "20.06.2020_11:28:44"
    val a = s.matches("\\d{2}\\.\\d{2}\\.\\d{4}.*")
    // Проверяем форматы типа "Fri,14_Aug_2020_21:45:44+0300"
    val b = s.contains("_") && s.matches(".*\\d{2}:\\d{2}:\\d{2}.*")
    // Более общая проверка на дату
    val c = s.matches(".*\\d{4}.*") && (s.contains("_") || s.contains(","))
    // Дополнительная проверка на числовые timestamp (например, Unix time)
    val d = s.matches("\\d{10,13}")
    a || b || c || d
  }

  // Проверяет, похож ли токен на docId (например, LAW_12345)
  private def isLikelyDocId(s: String): Boolean = {
    if (s == null || s.isEmpty) return false
    val hasUnderscore = s.contains("_")
    val hasLetter = s.exists(_.isLetter)
    hasUnderscore && hasLetter
  }

  // Форматы английских дат для разбора
  private val enFormats = Array(
    "EEE,_dd_MMM_yyyy_HH:mm:ss_Z",
    "EEE,_dd_MMM_yyyy_HH:mm:ssZ",
    "dd_MMM_yyyy_HH:mm:ss_Z",
    "dd_MMM_yyyy_HH:mm:ssZ"
  )

  // Пытается распарсить английскую дату
  private def tryParseEnglish(ts: String): Option[Date] = {
    if (ts == null) return None
    val normalized = ts.replaceAll(",", "")
    val cleaned = normalized.replaceAll("__", "_")
    enFormats.iterator.flatMap { fmt =>
      try {
        val sdf = new SimpleDateFormat(fmt, Locale.ENGLISH)
        Some(sdf.parse(cleaned))
      } catch {
        case _: Throwable => None
      }
    }.toSeq.headOption
  }

  // Извлекает дату в формате dd.MM.yyyy из строки с timestamp
  private def extractDateFromTimestamp(ts: String): String = {
    if (ts == null) return "unknown"
    val t = ts.trim
    try {
      if (t.matches("\\d{2}\\.\\d{2}\\.\\d{4}.*")) {
        // Простой случай: "20.06.2020_11:28:44"
        val idx = t.indexOf('_')
        if (idx > 0) t.substring(0, idx) else t
      } else if (t.matches(".*[A-Za-z]{3}.*")) {
        // Английский формат: "Fri,14_Aug_2020_21:45:44+0300"
        tryParseEnglish(t) match {
          case Some(d) =>
            val out = new SimpleDateFormat("dd.MM.yyyy")
            out.format(d)
          case None => "unknown"
        }
      } else if (t.contains("_") && t.matches(".*\\d{2}:\\d{2}:\\d{2}.*")) {
        val idx = t.indexOf('_')
        if (idx > 0) t.substring(0, idx) else "unknown"
      } else {
        "unknown"
      }
    } catch {
      case _: Throwable => "unknown"
    }
  }

  def main(args: Array[String]): Unit = {
    // Инициализация SparkSession
    val spark = SparkSession.builder()
      .appName("Session MapReduce Task")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    // Пути по умолчанию с возможностью переопределения через args
    val inputPath = if (args.length >= 1) args(0) else "data/Сессии/*"
    val outputDir = if (args.length >= 2) args(1) else "result"
    val dropUnknownDates = false // если true — строки с date="unknown" не будут сохраняться в выход

    // Читаем строки из входного файла
    val lines = sc.textFile(inputPath)

    val sessionStartPrefix = "SESSION_START"
    val sessionEndPrefix = "SESSION_END"
    val qsPrefix = "QS"
    val cardStartPrefix = "CARD_SEARCH_START"
    val cardEndPrefix = "CARD_SEARCH_END"
    val docOpenPrefix = "DOC_OPEN"

    // Собираем строки, привязанные к сессии (между SESSION_START и SESSION_END)
    val sessionLines = lines.mapPartitions(iter => {
      import scala.collection.mutable.ListBuffer
      var currentSession: Option[String] = None
      val out = ListBuffer.empty[(String, String)]
      while (iter.hasNext) {
        val line = iter.next()
        if (line.startsWith(sessionStartPrefix)) {
          val parts = safeSplit(line, 2)
          if (parts.length >= 2 && parts(1).nonEmpty)
            currentSession = Some(parts(1))
          else
            currentSession = Some("sess_unknown_" + java.util.UUID.randomUUID().toString)
        } else if (line.startsWith(sessionEndPrefix)) {
          currentSession = None
        } else {
          currentSession.foreach(id => out += ((id, line)))
        }
      }
      out.iterator
    })

    // Группируем по sessionId
    val sessionGrouped = sessionLines.groupByKey()

    val perSessionResults = sessionGrouped.flatMap { case (_sessionId, iterLines) =>
      val qsDocs = scala.collection.mutable.Set.empty[String]
      var localQSCount = 0
      var localCardCount = 0
      var acc45616CardSearchCount = 0 // Счетчик для ACC_45616 в карточных поисках
      var inCard = false
      val cardBuf = scala.collection.mutable.ListBuffer.empty[String]
      val docOpens = scala.collection.mutable.ListBuffer.empty[(String, String)] // (docId, timestamp)

      val it = iterLines.iterator
      while (it.hasNext) {
        val raw = it.next()
        val line = if (raw == null) "" else raw.trim
        if (line.isEmpty) {
          // пропускаем пустые
        } else if (line.startsWith(qsPrefix)) {
          localQSCount += 1
          val toks = safeSplit(line)
          val withoutPrefix = toks.drop(1)
          // удаляем блоки запросов {...} из токенов
          val cleaned = scala.collection.mutable.ListBuffer.empty[String]
          var skip = false
          withoutPrefix.foreach { t =>
            if (t.startsWith("{")) skip = true
            if (!skip) cleaned += t
            if (t.endsWith("}")) skip = false
          }
          cleaned.foreach { t =>
            if (isLikelyDocId(t)) qsDocs += norm(t)
          }
        } else if (line.startsWith(cardStartPrefix)) {
          inCard = true
          localCardCount += 1
          cardBuf.clear()
        } else if (line.startsWith(cardEndPrefix) && inCard) {
          inCard = false
          if (cardBuf.nonEmpty) cardBuf.foreach(d => if (isLikelyDocId(d)) qsDocs += norm(d))
          cardBuf.clear()
        } else if (inCard) {
          // Ищем строки внутри CARD_SEARCH блока
          if (line.startsWith("$0")) {
            val parts = safeSplit(line)
            if (parts.length >= 2 && norm(parts(1)) == "ACC_45616") {
              acc45616CardSearchCount += 1
            }
          }
          val toks = safeSplit(line)
          toks.foreach(t => if (t.nonEmpty) cardBuf += t)
        } else if (line.startsWith(docOpenPrefix)) {
          val toks = safeSplit(line)
          if (toks.length >= 3) { // Минимум: DOC_OPEN + ts + docId
            val tsCandidate = toks(1)
            val docCandidate = toks.last
            if (isLikelyDocId(docCandidate)) {
              val ts = if (isLikelyTimestamp(tsCandidate)) tsCandidate else "unknown_timestamp"
              docOpens += ((norm(docCandidate), ts))
            } else {
              println(s"Bad DOC_OPEN (invalid docId format): [$line]")
            }
          } else {
            println(s"Bad DOC_OPEN (too few tokens): [$line]")
          }
        } else {
          // После CARD_SEARCH_END возможна строка с searchId и docId'ами
          val toks = safeSplit(line)
          if (toks.length >= 2 && toks(0).forall(_.isDigit)) {
            val docs = toks.drop(1)
            docs.foreach(d => if (isLikelyDocId(d)) qsDocs += norm(d))
          }
        }
      }

      // Формируем выходные данные: (дата, docId) -> количество
      val out = scala.collection.mutable.ListBuffer.empty[((String, String), Int)]
      docOpens.foreach { case (docId, ts) =>
        if (qsDocs.contains(docId)) {
          val date = extractDateFromTimestamp(ts)
          if (date != "unknown" || !dropUnknownDates) {
            val keyDate = if (date != "unknown") date else "unknown"
            out += (((keyDate, docId), 1))
          }
        }
      }
      if (localQSCount > 0) out += ((("__META__", "QS_COUNT"), localQSCount))
      if (localCardCount > 0) out += ((("__META__", "CARD_COUNT"), localCardCount))
      if (acc45616CardSearchCount > 0) out += ((("__META__", "ACC_45616_CARD_SEARCH"), acc45616CardSearchCount))

      out.iterator
    }

    // Суммируем по ключам
    val aggregated = perSessionResults.reduceByKey(_ + _)
    val metaCounts = aggregated.filter { case ((date, _), _) => date == "__META__" }
    val realCounts = aggregated.filter { case ((date, _), _) => date != "__META__" }
    val metaMap = metaCounts.collect().toMap
    val totalQS = metaMap.get(("__META__", "QS_COUNT")).map(_.toLong).getOrElse(0L)
    val totalCardSearch = metaMap.get(("__META__", "CARD_COUNT")).map(_.toLong).getOrElse(0L)
    val acc45616CardSearches = metaMap.get(("__META__", "ACC_45616_CARD_SEARCH")).map(_.toLong).getOrElse(0L)

    println(s"(Отладка) Всего быстрых поисков (QS): $totalQS")
    println(s"(Отладка) Всего карточечных поисков (CARD_SEARCH): $totalCardSearch")
    println(s"Количество раз, когда в карточке производили поиск документа ACC_45616: $acc45616CardSearches")

    // Путь и файловая система для вывода
    val outputPath = new Path(outputDir)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    if (fs.exists(outputPath)) fs.delete(outputPath, true)

    // Сохраняем результат в формате "дата docId количество"
    realCounts
      .map { case ((date, docId), cnt) => s"$date\t$docId\t$cnt" }
      .coalesce(1)
      .saveAsTextFile(outputDir)

    // Переименование part- файла в opens.txt
    try {
      val files = fs.listStatus(outputPath).map(_.getPath.getName).filter(_.startsWith("part-"))
      if (files.nonEmpty) {
        val part = files.head
        val src = new Path(outputDir + "/" + part)
        val dst = new Path(outputDir + "/opens.txt")
        if (fs.exists(dst)) fs.delete(dst, true)
        fs.rename(src, dst)
        println(s"Количество открытий каждого документа, найденного через быстрый поиск за каждый день -> ${outputDir}/opens.txt")
      } else {
        println("Не найден part- файл в выведенной папке (возможно пустой RDD).")
      }
    } catch {
      case e: Exception => println(s"Не удалось переименовать part-файл: ${e.getMessage}")
    }

    spark.stop()
  }
}
