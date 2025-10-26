package org.example.parser.model

import java.time.LocalDateTime

abstract class Event(val datetime: Option[LocalDateTime]) {
  // Каждое событие само должно знать, как себя "положить"
  protected def addToSession(ctx: ParseContext): Unit

  // Добавление в контекст: добавление события в сессию (обязательно) + что-то еще
  private[model] def addToContext(ctx: ParseContext): Unit =
    addToSession(ctx)
}

trait EventObject[T <: Event] {
  protected val prefix = ""
  protected val postfix = ""

  // Проверка на начало события (по умолчанию - если строка начинается с префикса события)
  def matches(line: String): Boolean = line.startsWith(prefix)

  // Возвращаем объект Event (точнее "объект-наследник" Event)
  def parse(ctx: ParseContext): T
}
