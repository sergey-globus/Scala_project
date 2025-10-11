package org.example.infrastructure

import org.example.domain._

import java.time._
import java.time.format._
import scala.util.Try

object Parser {

  // Проверка валидности docId: <БАЗА>_<номер>
  private def isValidDocId(token: String): Boolean =
    token != null && token.matches("""[A-Z0-9]{1,15}_\d{1,10}""")

  // Разбор даты из datetime
  private def extractDateFromDatetime(dt: String): String = {
    if (dt == null || dt.trim.isEmpty || dt == "unknown") return "unknown"

    val format1 = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
    val format2 = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_Z", java.util.Locale.ENGLISH)

    Try(LocalDateTime.parse(dt, format1))
      .toOption.map(_.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")))

      .orElse(Try(ZonedDateTime.parse(dt, format2))
        .toOption.map(_.toLocalDate.format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))))

      .getOrElse("invalid")
  }


  def parseSession(fileName: String, lines: Iterator[String]): Session = {
    try {
      Session.parse(fileName, lines, isValidDocId, extractDateFromDatetime, Logger.logUnknown)
    } catch {
      // при исключении продолжаем вычисления без "ошибочной" сессии
      case ex: Throwable =>
        Logger.logException(fileName, ex, "parseSession failed")
        Session.empty(fileName)
    }
  }


}
