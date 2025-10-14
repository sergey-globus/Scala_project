// src/test/scala/org/example/infrastructure/TestParser.scala
package org.example.infrastructure

import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._ // для Scala 2.12

class TestParser extends AnyFunSuite {

  private def debugSessionToString(session: org.example.domain.Session): String = {
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
        val openDocs = cs.getOpenDocs
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
        val openDocs = qs.getOpenDocs
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

  test("Парсер должен корректно разбирать все сессии по событиям") {
    val resourcesRootUrl = getClass.getResource("/")
    assert(resourcesRootUrl != null, "Папка ресурсов не найдена!")

    val resourcesRoot = Paths.get(resourcesRootUrl.toURI)
    val resultDir = Paths.get("src/test/result")
    if (!Files.exists(resultDir)) Files.createDirectories(resultDir)

    // Проходим по всем файлам в папке ресурсов
    Files.list(resourcesRoot).iterator().asScala.foreach { filePath =>
      if (Files.isRegularFile(filePath)) {
        val lines = Files.readAllLines(filePath).asScala.iterator
        assert(lines.nonEmpty, s"Файл ${filePath.getFileName} пустой")

        val session = Parser.parseSession(filePath.getFileName.toString, lines)
        // Проверяем, что QS и DocOpen правильно распарсились
        assert(session.quickSearches.nonEmpty || session.cardSearches.nonEmpty, s"Файл ${filePath.getFileName}: нет поисковых действий")
        assert(session.docOpens.nonEmpty, s"Файл ${filePath.getFileName}: нет открытых документов")

        // Создаём debug-файл в src/test/result
        val debugPath = resultDir.resolve(s"${filePath.getFileName.toString}.txt")
        val debugStr = debugSessionToString(session)
        Files.write(debugPath, debugStr.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      }
    }
  }
}
