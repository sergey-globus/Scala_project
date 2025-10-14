package org.example.infrastructure

import org.example.domain.Session

import java.time._
import java.time.format._
import scala.util.Try

object Parser {

  // Проверка валидности docId: <БАЗА>_<номер>
  private def isValidDocId(token: String): Boolean =
    token != null && token.matches("""[A-Z0-9]{1,15}_\d{1,10}""")

  private def extractDatetime(dt: String): Option[LocalDateTime] = {
    val format1 = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
    val format2 = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_Z", java.util.Locale.ENGLISH)

    // Пробуем format1
    val local = Try(LocalDateTime.parse(dt, format1)).toOption
    // Пробуем format2
    val zoned = Try(ZonedDateTime.parse(dt, format2)).toOption.map(_.toLocalDateTime)

    // Возвращаем первый успешный результат (либо None)
    local.orElse(zoned)
  }

  def parseSession(fileName: String, lines: Iterator[String]): Session = {
    try {
      Session.parse(fileName, lines, isValidDocId, extractDatetime, Logger.logUnknown)
    } catch {
      // при исключении продолжаем вычисления без "ошибочной" сессии
      case ex: Throwable =>
        Logger.logException(fileName, ex, "parseSession failed")
        Session.empty(fileName)
    }
  }
}
