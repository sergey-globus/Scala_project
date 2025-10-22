package org.example.checker

import java.time._
import java.time.format._
import scala.util.Try

object Validator {

  // Проверка валидности docId: <БАЗА>_<номер>
  def isValidDocId(token: String): Boolean =
    token != null && token.matches("""[A-Z0-9]{1,15}_\d{1,10}""")

  def extractDatetime(dt: String): LocalDateTime = {
    val format1 = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
    val format2 = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_Z", java.util.Locale.ENGLISH)

    Try(LocalDateTime.parse(dt, format1))
      .orElse(Try(ZonedDateTime.parse(dt, format2).toLocalDateTime))
      .getOrElse(null)
  }
}
