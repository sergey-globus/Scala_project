package org.example.infrastructure

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

object EventParser {

  def safeSplit(s: String, limit: Int = -1): Array[String] =
    if (s == null) Array.empty else s.split("\\s+", limit).map(_.trim)

  def norm(s: String): String =
    if (s == null) "" else s.trim.toUpperCase

  def isLikelyTimestamp(s: String): Boolean =
    s != null && (s.matches("\\d{2}\\.\\d{2}\\.\\d{4}.*") || s.contains("_") || s.matches("\\d{10,13}"))

  def isLikelyDocId(s: String): Boolean =
    s != null && s.contains("_") && s.exists(_.isLetter)

  private val enFormats = Array(
    "EEE,_dd_MMM_yyyy_HH:mm:ss_Z",
    "EEE,_dd_MMM_yyyy_HH:mm:ssZ",
    "dd_MMM_yyyy_HH:mm:ss_Z",
    "dd_MMM_yyyy_HH:mm:ssZ"
  )

  def tryParseEnglish(ts: String): Option[Date] = {
    if (ts == null) return None
    val normalized = ts.replaceAll(",", "")
    val cleaned = normalized.replaceAll("__", "_")
    enFormats.iterator.flatMap { fmt =>
      try {
        Some(new SimpleDateFormat(fmt, Locale.ENGLISH).parse(cleaned))
      } catch {
        case _: Throwable => None
      }
    }.toSeq.headOption
  }

  def extractDateFromTimestamp(ts: String): String = {
    if (ts == null) return "unknown"
    val t = ts.trim
    try {
      if (t.matches("\\d{2}\\.\\d{2}\\.\\d{4}.*")) {
        val idx = t.indexOf('_')
        if (idx > 0) t.substring(0, idx) else t
      } else if (t.matches(".*[A-Za-z]{3}.*")) {
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
}
