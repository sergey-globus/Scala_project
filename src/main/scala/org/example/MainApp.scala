package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer

object MainApp {

  // Абстрактный трейт для событий с минимально обязательными полями
  sealed trait Event {
    def timestamp: String             // время события (строка)
    def sessionId: Option[String]    // идентификатор сессии, может отсутствовать
  }

  // Начало сессии — timestamp, без sessionId
  case class SessionStart(timestamp: String) extends Event {
    override def sessionId: Option[String] = None
  }

  // Конец сессии — также только timestamp
  case class SessionEnd(timestamp: String) extends Event {
    override def sessionId: Option[String] = None
  }

  // Быстрый поиск (QS), содержит timestamp, sessionId и список документов (docs)
  case class QS(timestamp: String, sessionIdValue: String, docs: Seq[String]) extends Event {
    override def sessionId: Option[String] = Some(sessionIdValue)
  }

  // Поиск по карточкам (CardSearch), аналогично QS
  case class CardSearch(timestamp: String, sessionIdValue: String, docs: Seq[String]) extends Event {
    override def sessionId: Option[String] = Some(sessionIdValue)
  }

  // Открытие документа (DocOpen), содержит timestamp, sessionId (опционально) и один docId
  case class DocOpen(timestamp: String, sessionIdValue: Option[String], docId: String) extends Event {
    override def sessionId: Option[String] = sessionIdValue
  }

  // Хранит события одной сессии с её id
  case class Session(id: String, events: ListBuffer[Event])

  def main(args: Array[String]): Unit = {
    // Инициализация SparkSession для работы с DataFrame и Spark SQL
    val spark = SparkSession.builder()
      .appName("MyTestTaskApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Путь к директории с логами
    val logPath = "target/classes/Сессии/*"

    // Читаем все строки из логов в память !!!
    // Для больших данных можно оптимизировать, используя DataFrame без collect
    val rawLines = spark.read.textFile(logPath).collect()

    // Коллекция сессий: sessionId -> Session (хранит события)
    val sessions = scala.collection.mutable.Map[String, Session]()

    // Текущий id сессии, определяемый по SESSION_START
    var currentSessionId: Option[String] = None

    // Состояние для карточечного поиска между CARD_SEARCH_START и CARD_SEARCH_END:
    // Содержит (sessionId, timestamp, буфер для документов)
    var cardSearchInProgress: Option[(String, String, ListBuffer[String])] = None

    /**
     * Функция для извлечения даты в формате дд.мм.гггг из timestamp'а.
     * Работает с двумя основными форматами в логах, если не совпадает, возвращает None.
     */
    def extractDate(timeStr: String): Option[String] = {
      if (timeStr == null) return None
      val regex1 = """(\d{2}\.\d{2}\.\d{4})_.*""".r
      val regex2 = """[A-Za-z]{3},_([0-9]{2}_[A-Za-z]{3}_[0-9]{4})_.*""".r

      timeStr match {
        case regex1(d) => Some(d)
        case regex2(d) =>
          val parts = d.split("_")
          if (parts.length == 3) {
            val monthMap = Map(
              "Jan" -> "01", "Feb" -> "02", "Mar" -> "03", "Apr" -> "04", "May" -> "05", "Jun" -> "06",
              "Jul" -> "07", "Aug" -> "08", "Sep" -> "09", "Oct" -> "10", "Nov" -> "11", "Dec" -> "12"
            )
            val day = parts(0)
            val mon = monthMap.getOrElse(parts(1), "01")
            val year = parts(2)
            Some(s"$day.$mon.$year")
          } else None
        case _ => None
      }
    }

    /**
     * Безопасный сплит строки на слова с обрезкой пробелов.
     * Если вход null — возвращает пустой массив.
     */
    def safeSplit(str: String, limit: Int = -1): Array[String] = {
      if (str == null) Array.empty[String] else str.split(" ", limit).map(_.trim)
    }


    // Начинаем обработку каждой строки из логов
    for(line <- rawLines) {
      val parts = safeSplit(line, 3)          // разбиваем на три части (тип_события, timestamp, остальное)
      val eventType = if(parts.nonEmpty) parts(0) else ""

      eventType match {

        // Начало сессии — записываем currentSessionId и создаём сессию
        case "SESSION_START" =>
          if(parts.length >= 2) {
            currentSessionId = Some(parts(1))
            sessions(currentSessionId.get) = Session(currentSessionId.get, ListBuffer())
            sessions(currentSessionId.get).events += SessionStart(parts(1))
          }

        // Конец сессии — добавляем событие и сбрасываем currentSessionId
        case "SESSION_END" =>
          if(parts.length >= 2 && currentSessionId.isDefined) {
            sessions(currentSessionId.get).events += SessionEnd(parts(1))
            currentSessionId = None
          }

        // Быстрый поиск (QS)
        case "QS" =>
          if(currentSessionId.isDefined && parts.length >= 3) {
            val timestamp = parts(1)
            val rest = parts(2)
            val tokens = safeSplit(rest)
            // При необходимости пропускаем первый токен если это '{...}'
            val startIdx = if(tokens.nonEmpty && tokens.head.startsWith("{")) 1 else 0
            if(tokens.length >= startIdx + 1) {
              val sessIdInLine = currentSessionId.get    // Используем текущий sessionId из SESSION_START
              val docs = if(tokens.length > startIdx + 1) tokens.drop(startIdx + 1) else Array.empty[String]
              sessions(sessIdInLine).events += QS(timestamp, sessIdInLine, docs.toSeq)
            }
          }

        // Начало карточечного поиска — запускаем сбор документов
        case "CARD_SEARCH_START" =>
          if(currentSessionId.isDefined && parts.length >= 2) {
            cardSearchInProgress = Some((currentSessionId.get, parts(1), ListBuffer.empty[String]))
          }

        // Конец карточечного поиска — формируем событие с накопленными документами
        case "CARD_SEARCH_END" =>
          cardSearchInProgress match {
            case Some((sessId, ts, docsBuffer)) =>
              sessions(sessId).events += CardSearch(ts, sessId, docsBuffer.toSeq)
              cardSearchInProgress = None
            case None =>
            // Нет открытого карточечного поиска — игнорируем
          }

        // Все прочие строки — если собираем docs карточечного поиска — добавляем документы
        case _ =>
          if(cardSearchInProgress.isDefined){
            val (sessId, ts, docsBuffer) = cardSearchInProgress.get
            val tokens = safeSplit(line)
            docsBuffer ++= tokens
          }
          // Обработка открытия документа
          if(line.startsWith("DOC_OPEN")){
            val spl = safeSplit(line, 4) // DOC_OPEN timestamp sessionId docId
            if(spl.length >= 4 && currentSessionId.isDefined) {
              val timestamp = spl(1)
              val sessIdFromLine = spl(2)  // из строки, но НЕ используем напрямую для сессии
              val docId = spl(3)
              // Добавляем событие DocOpen в сессию, определяемую currentSessionId
              sessions(currentSessionId.get).events += DocOpen(timestamp, currentSessionId, docId)
            }
          }
      }
    }

    // --- Задача 1 ---
    // Подсчёт количества поисков документа ACC_45616 через карточечный поиск
    val countCardSearchAcc45616 = sessions.values.flatMap(_.events).collect {
      case CardSearch(_, _, docs) if docs.exists(_.trim.equalsIgnoreCase("ACC_45616")) => 1
    }.sum

    println(s"Count of ACC_45616 card searches: $countCardSearchAcc45616")

    // --- Задача 2 ---
    // Собираем все события QS в плоский формат
    val allQS = sessions.values.flatMap(_.events.collect {
      case qs: QS => (qs.sessionId.getOrElse("").trim.toLowerCase, qs.timestamp, qs.docs.map(_.trim.toLowerCase))
    }).toSeq

    // Аналогично собираем открытие документов DocOpen
    val allDocOpen = sessions.values.flatMap(_.events.collect {
      case docOpen: DocOpen => (docOpen.sessionId.getOrElse("").trim.toLowerCase, docOpen.docId.trim.toLowerCase, docOpen.timestamp)
    }).toSeq

    println(s"Total QS events: ${allQS.size}")
    println(s"Total DOC_OPEN events: ${allDocOpen.size}")

    // Создаём DataFrame для QS и DOC_OPEN
    val qsDF = spark.createDataFrame(allQS).toDF("sessionId", "timestamp", "docs")
    val docOpenDF = spark.createDataFrame(allDocOpen).toDF("sessionId", "docId", "timestamp")

    // Разворачиваем массив docs в QS по одному docId в строке
    val explodeDocs = qsDF.withColumn("docId", explode($"docs"))

    // UDF для извлечения даты из timestamp
    val extractDateUdf = udf { (ts: String) =>
      Option(ts).flatMap(extractDate).getOrElse("unknown")
    }

    // Добавляем нормализованные колонки и дату в QS
    val qsWithDate = explodeDocs
      .withColumn("date", extractDateUdf($"timestamp"))
      .withColumn("normSessionId", lower(trim($"sessionId")))
      .withColumn("normDocId", lower(trim($"docId")))
      .select("normSessionId", "normDocId", "date")
      .distinct()

    // Аналогично для DOC_OPEN
    val docOpenWithDate = docOpenDF
      .withColumn("date", extractDateUdf($"timestamp"))
      .withColumn("normSessionId", lower(trim($"sessionId")))
      .withColumn("normDocId", lower(trim($"docId")))
      .select("normSessionId", "normDocId", "date", "timestamp")

    // Соединяем по нормализованным sessionId и docId (inner join)
    val joined = docOpenWithDate.join(qsWithDate,
      docOpenWithDate("normSessionId") === qsWithDate("normSessionId") &&
        docOpenWithDate("normDocId") === qsWithDate("normDocId"),
      "inner"
    )

    // Убираем дублирование колонки date - явное указание колонок
    val joinedSelected = joined.select(
      docOpenWithDate("normSessionId").as("sessionId"),
      docOpenWithDate("normDocId").as("docId"),
      docOpenWithDate("timestamp"),
      docOpenWithDate("date").as("date")
    )

    println("Sample matched DOC_OPEN and QS events by sessionId and docId:")
    joinedSelected.show(20, truncate = false)

    // Группируем по дате и документу, считаем количество
    val result = joinedSelected.groupBy($"date", $"docId")
      .count()
      .orderBy($"date", $"docId")

    println("Count of document openings found through quick search per day:")
    result.show(100, truncate = false)

    spark.stop()
  }
}
