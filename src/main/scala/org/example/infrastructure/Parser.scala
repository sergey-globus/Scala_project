package org.example.infrastructure

import org.example.domain._
import scala.collection.mutable
import java.nio.file.Paths

object Parser {

  // Проверка валидности docId: <БАЗА>_<номер>
  private def isValidDocId(token: String): Boolean =
    token != null && token.matches("""[A-Z0-9]{1,15}_\d{1,10}""")

  // Разбор даты из timestamp
  def extractDateFromTimestamp(ts: String): String = {
    if (ts == null || ts.trim.isEmpty || ts == "unknown") return "unknown"
    val datePattern = raw"(\d{2}\.\d{2}\.\d{4})(?:_.*)?".r
    ts match {
      case datePattern(date) => date // дата без времени
      case _ => "invalid"
    }
  }


  // Основной парсер
  def parseSession(lines: Iterable[(String, String)], logUnknown: (String, String) => Unit): Session = {

    // Состояния
    var currentCardId = "unknown"
    var currentCardTimestamp = "unknown"
    var currentCardParams = mutable.ListBuffer.empty[(Int, String)]
    var currentCardDocs = mutable.ListBuffer.empty[String]

    var currentQSId = "unknown"
    var currentQSTimestamp = "unknown"
    var currentQuery = ""
    var currentQSDocs = mutable.ListBuffer.empty[String]

    val cardSearches = mutable.ListBuffer.empty[CardSearch]
    val qsQueries = mutable.ListBuffer.empty[QuerySearch]
    val docOpens = mutable.ListBuffer.empty[DocOpen]

    var inCard = false
    var inQS = false
    var afterCardEnd = false

    for ((filePath, lineRaw) <- lines) {
      val fileName = Paths.get(filePath).getFileName.toString
      val line = Option(lineRaw).getOrElse("").trim
      if (line.isEmpty) {
        // пропускаем пустые строки
      }

      // --- CARD_SEARCH_START ---
      else if (line.startsWith("CARD_SEARCH_START")) {
        inCard = true
        afterCardEnd = false
        currentCardTimestamp = line.split("\\s+").lift(1).getOrElse("unknown")
      }

      // --- CARD_SEARCH_END ---
      else if (line.startsWith("CARD_SEARCH_END")) {
        inCard = false
        afterCardEnd = true
      }

      // --- внутри CARD_SEARCH ---
      else if (inCard) {
        if (line.startsWith("$")) {
          val parts = line.split("\\s+", 2)
          if (parts.length == 2) {
            import scala.util.Try
            val numOpt = Try(parts(0).drop(1).toInt).toOption
            val text = parts(1)
            numOpt match {
              case Some(num) => currentCardParams += ((num, text))
              case None => logUnknown(fileName, s"Bad CARD param (invalid number): $line")
            }
          } else {
            logUnknown(fileName, s"Bad CARD param (missing text): $line")
          }
        }
      }

      // --- сразу после CARD_SEARCH_END — найденные документы ---
      else if (afterCardEnd && line.nonEmpty) {
        val toks = line.split("\\s+")
        currentCardId = toks.headOption.getOrElse("unknown")
        currentCardDocs ++= toks.tail.filter(isValidDocId)
        cardSearches += CardSearch(
          currentCardId,
          currentCardTimestamp,
          currentCardParams.toSeq,
          currentCardDocs.toSeq
        )
        currentCardParams.clear()
        currentCardDocs.clear()
        afterCardEnd = false
      }

      // --- QS (поиск с текстом) ---
      else if (line.startsWith("QS")) {
        val pattern = raw"QS\s+(\S+)\s+\{(.*)\}".r
        line match {
          case pattern(ts, query) =>
            inQS = true
            currentQSTimestamp = ts
            currentQuery = query
            currentQSDocs.clear()
          case _ =>
            logUnknown(fileName, s"Bad QS line: $line")
        }
      }

      // --- строка после QS: QSId + найденные документы ---
      else if (inQS) {
        val toks = line.split("\\s+")
        if (toks.nonEmpty) {
          currentQSId = toks.head
          currentQSDocs ++= toks.tail.filter(isValidDocId)
          qsQueries += QuerySearch(
            currentQSId,
            currentQSTimestamp,
            currentQuery,
            currentQSDocs.toSeq
          )
          currentQSId = "unknown"
          currentQSTimestamp = "unknown"
          currentQuery = ""
          currentQSDocs.clear()
          inQS = false
        }
      }

      // --- DOC_OPEN ---
      else if (line.startsWith("DOC_OPEN")) {
        val toks = line.split("\\s+")
        val (tsCandidate, searchId, docCandidate) = toks.length match {
          case 4 => (toks(1), toks(2), toks(3))   // формат с датой
          case 3 => ("unknown", toks(1), toks(2))  // формат без даты
          case _ => ("unknown", "unknown", "unknown")
        }

        val ts = extractDateFromTimestamp(tsCandidate)
        val docId = if (isValidDocId(docCandidate)) docCandidate else "unknown"
        if (ts == "invalid") logUnknown(fileName, s"Unknown date: $tsCandidate")
        if (docId == "unknown") logUnknown(fileName, s"Unknown date: $docCandidate")

        docOpens += DocOpen(ts, searchId, docCandidate)
      }
    }

    // Собираем финальную сессию
    val sessionId = Paths.get(lines.headOption.map(_._1).getOrElse("unknown")).getFileName.toString
    Session(sessionId, cardSearches.toSeq, qsQueries.toSeq, docOpens.toSeq)
  }
}
