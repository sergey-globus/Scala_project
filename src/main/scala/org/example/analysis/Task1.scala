package org.example.analysis

import org.apache.spark.rdd.RDD
import org.example.parser.model.Session

/**
 * Количество раз, когда в карточке производили поиск документа с идентификатором ACC_45616
 */
object Task1 {

  private val targets = Array("acc_45616", "асс_45616") // на английском и на русском

  def run(sessions: RDD[Session]): Unit = {
    val targetCount = sessions.filter { session =>
      session.cardSearches.exists { cs =>
        cs.params.exists { case (num, text) =>
          (num == 0 || num == 134) &&
            targets.exists(text.toLowerCase.contains)
        }
      }
    }.count()

    println(s"(Task1) ACC_45616 CARD_SEARCH count: $targetCount")
  }
}
