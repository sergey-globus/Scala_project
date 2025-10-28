package org.example.parser.model

import java.time._
import java.time.format._
import scala.util.Try

object DatetimeParser {

  def parseDatetime(dt: String): Option[LocalDateTime] = {
    val format1 = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
    val format2 = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_Z", java.util.Locale.ENGLISH)

    Try(LocalDateTime.parse(dt, format1)).map(Some(_))
      .orElse(Try(ZonedDateTime.parse(dt, format2).toLocalDateTime).map(Some(_)))
      .getOrElse(None)
  }
}
