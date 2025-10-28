package org.example.parser.model

import java.time.LocalDateTime

abstract class Event(val datetime: Option[LocalDateTime])

trait EventObject[T <: Event] {
  protected val prefix = ""
  protected val postfix = ""

  // Каждое событие само должно знать, как себя "положить"
  protected def addToSession(ctx: ParseContext, event: T): Unit

  // Добавление в контекст: добавление события в сессию (обязательно) + что-то еще
  def addToContext(ctx: ParseContext, event: T): Unit =
    addToSession(ctx, event)

  // Проверка на начало события (по умолчанию - если строка начинается с префикса события)
  def matches(line: String): Boolean = line.startsWith(prefix)

  // Возвращаем объект Event (точнее "объект-наследник" Event)
  def parse(ctx: ParseContext): T
}
