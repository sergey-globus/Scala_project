package org.example.domain

case class CardSearch(
                       cardId: String,
                       timestamp: String,
                       params: Seq[(Int, String)],
                       foundDocs: Seq[String])
case class QuerySearch(
                        QSId: String,
                        timestamp: String,
                        query: String,
                        foundDocs: Seq[String])
case class DocOpen(
                    timestamp: String,
                    cardOrQSId: String,
                    docId: String)
case class Session(
                    sessionId: String,
                    cardSearches: Seq[CardSearch],
                    qsQueries: Seq[QuerySearch],
                    docOpens: Seq[DocOpen]
                  )
