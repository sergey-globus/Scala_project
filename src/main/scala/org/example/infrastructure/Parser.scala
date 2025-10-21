package org.example.infrastructure

import org.example.domain.{Session, ParseContext}

import java.time._
import java.time.format._
import scala.util.Try

object Parser {

  // Проверка валидности docId: <БАЗА>_<номер>
  private def isValidDocId(token: String): Boolean =
    token != null && token.matches("""[A-Z0-9]{1,15}_\d{1,10}""")

  private def extractDatetime(dt: String): LocalDateTime = {
    val format1 = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
    val format2 = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_Z", java.util.Locale.ENGLISH)

    Try(LocalDateTime.parse(dt, format1))
      .orElse(Try(ZonedDateTime.parse(dt, format2).toLocalDateTime))
      .getOrElse(null)
  }

  def parseSession(fileName: String, lines: Iterator[String], logAcc: Logger): Session = {
    try {
      Session.parse(fileName, lines, isValidDocId, extractDatetime, logAcc.add, logAcc.addException)
    } catch {
      // при исключении продолжаем вычисления без "ошибочной" сессии
      case ex: Throwable =>
        logAcc.addException(fileName, ex, "Parsing session failed")
        Session.empty(fileName)
    }
  }
}
