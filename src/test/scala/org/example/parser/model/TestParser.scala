// src/test/scala/org/example/infrastructure/TestParser.scala
package org.example.parser.model

import org.example.checker.Logger
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.JavaConverters._
import scala.io.Source

case class ExpectedSessionStats(
                                 quickSearchCount: Int,
                                 cardSearchCount: Int,
                                 docOpenCount: Int
                               )

class TestParser extends AnyFunSuite {

  private def loadExpected(): Map[String, ExpectedSessionStats] = {
    val path = Paths.get("src/test/expected.json")
    assert(Files.exists(path), s"Файл ожиданий не найден: $path")

    val source = Source.fromFile(path.toFile, "UTF-8")
    val jsonStr = source.mkString
    val json = ujson.read(jsonStr)
    source.close()

    json.obj.map { case (name, v) =>
      name -> ExpectedSessionStats(
        quickSearchCount = v("quickSearchCount").num.toInt,
        cardSearchCount  = v("cardSearchCount").num.toInt,
        docOpenCount     = v("docOpenCount").num.toInt
      )
    }.toMap
  }



  private def debugSessionToString(session: Session): String = {
    val sb = new StringBuilder
    sb.append(s"Session ID: ${session.id}\n\n")

    // Card Searches
    if (session.cardSearches.nonEmpty) {
      sb.append("Card Searches:\n")
      session.cardSearches.foreach { cs =>
        val paramsStr = if (cs.params.nonEmpty)
          cs.params.map { case (num, text) => s"($num, '$text')" }.mkString(", ")
        else "None"
        val foundDocsStr = if (cs.foundDocs.nonEmpty) cs.foundDocs.mkString(", ") else "None"
        val openDocs = cs.getDocOpens
        val openDocsStr = if (openDocs.nonEmpty) openDocs.mkString(", ") else "None"
        sb.append(s"id: ${cs.id}\n")
        sb.append(s"datetime: ${cs.datetime}\n")
        sb.append(s"params: $paramsStr\n")
        sb.append(s"foundDocs: $foundDocsStr\n")
        sb.append(s"openDocs: $openDocsStr\n---\n")
      }
    } else sb.append("No Card Searches found.\n")

    sb.append("=" * 50 + "\n")

    // Query Searches
    if (session.quickSearches.nonEmpty) {
      sb.append("Query Searches:\n")
      session.quickSearches.foreach { qs =>
        val foundDocsStr = if (qs.foundDocs.nonEmpty) qs.foundDocs.mkString(", ") else "None"
        val openDocs = qs.getDocOpens
        val openDocsStr = if (openDocs.nonEmpty) openDocs.mkString(", ") else "None"
        sb.append(s"id: ${qs.id}\n")
        sb.append(s"datetime: ${qs.datetime}\n")
        sb.append(s"query: ${qs.query}\n")
        sb.append(s"foundDocs: $foundDocsStr\n")
        sb.append(s"openDocs: $openDocsStr\n---\n")
      }
    } else sb.append("No Query Searches found.\n")

    sb.append("=" * 50 + "\n")

    // Doc Opens
    if (session.docOpens.nonEmpty) {
      sb.append("Doc Opens:\n")
      session.docOpens.foreach { doc =>
        sb.append(s"datetime: ${doc.datetime}\n")
        sb.append(s"searchId: ${doc.searchId}\n")
        sb.append(s"docId: ${doc.docId}\n---\n")
      }
    } else sb.append("No Doc Opens found.\n")

    sb.toString()
  }

  test("Парсер корректно разбирает все сессии и совпадает с ожиданиями") {
    val resourcesRootUrl = getClass.getResource("/")
    assert(resourcesRootUrl != null, "Папка ресурсов не найдена!")

    val resourcesRoot = Paths.get(resourcesRootUrl.toURI)
    val resultDir = Paths.get("src/test/result")
    if (!Files.exists(resultDir)) Files.createDirectories(resultDir)

    val logAcc = new Logger
    val expected = loadExpected()

    val errors = scala.collection.mutable.ListBuffer.empty[String]

    Files.list(resourcesRoot).filter(Files.isRegularFile(_)) // <-- чтоб файлы без расширения не считались директориями
      .iterator().asScala.foreach { filePath =>
      val lines = Files.readAllLines(filePath).asScala.iterator
      val fname = filePath.getFileName.toString

      val session = Session.parse(ParseContext(fname, lines, logAcc))

      expected.get(fname) match {
        case Some(exp) =>
          if (session.quickSearches.size != exp.quickSearchCount)
            errors += s"$fname: QS mismatch, expected ${exp.quickSearchCount}, actual ${session.quickSearches.size}"
          if (session.cardSearches.size != exp.cardSearchCount)
            errors += s"$fname: CS mismatch, expected ${exp.cardSearchCount}, actual ${session.cardSearches.size}"
          if (session.docOpens.size != exp.docOpenCount)
            errors += s"$fname: DocOpen mismatch, expected ${exp.docOpenCount}, actual ${session.docOpens.size}"

        case None =>
          info(s"Нет проверок для $fname — только debug сохранён")
      }

      val debugPath = resultDir.resolve(s"${filePath.getFileName.toString}.txt")
      val debugStr = debugSessionToString(session)
      Files.write(debugPath, debugStr.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    }

    // Выводим все ошибки и падаем, если они есть
    if (errors.nonEmpty) {
      println("Найдены ошибки в сессиях:")
      errors.foreach(println)
      fail(s"Всего ${errors.size} несоответствий")
    }
    else
      println("Все тесты пройдены!")
  }

}
