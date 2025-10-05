package org.example.parser

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

object LogParser {
  def extractDateFromTimestamp(ts: String): String = {
    if (ts == null || ts.isEmpty) return "unknown"
    try {
      if (ts.matches("\\d{2}\\.\\d{2}\\.\\d{4}.*")) {
        val idx = ts.indexOf('_')
        if (idx > 0) ts.substring(0, idx) else ts
      } else if (ts.contains("_") && ts.matches(".*\\d{2}:\\d{2}:\\d{2}.*")) {
        val idx = ts.indexOf('_')
        if (idx > 0) ts.substring(0, idx) else "unknown"
      } else "unknown"
    } catch {
      case _: Throwable => "unknown"
    }
  }
}
