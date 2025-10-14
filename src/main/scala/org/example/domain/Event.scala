package org.example.domain

trait Event {
  def addToSession(ctx: ParseContext): Unit
}

trait EventObject[T <: Event] {
  protected val prefix: String = ""
  protected val postfix: String = ""

  // Проверка на начало события (по умолчанию - если строка начинается с префикса события)
  def matches(line: String): Boolean = line.startsWith(prefix)

  // Возвращаем объект Event (точнее "объект-наследник" Event)
  def parse(ctx: ParseContext): T
}
