package org.example.domain

case class Session(
                    id: String,
                    docOpens: Seq[(String, String)], // (docId, timestamp)
                    qsCount: Int,
                    cardCount: Int,
                    acc45616Count: Int
                  )
