## Используемые версии зависимостей и плагинов

- Scala version: **2.12.18**  
- Spark version: **3.5.2**  
- Java compiler source/target: **11**

## Сборка и запуск

Для сборки проекта (JAR-файла с контейнером всех зависимостей) используется Maven:
```
mvn clean package
```

## В проекте используются следующие модули:

- model - содержит сущности, а также "знает" о том, как парсить события и сессию, чтобы их получить
- checker - вспомогательный пакет для model для валидации и логирования по ходу парсинга сессии
- parser - получение объектов сессии из исходных файлов
- analysis - оперирует объектами domain для вычисления каждой task
- main - входная точка приложения, запускает обработку и связывает остальные модули


### Строка с открытием документа DOC_OPEN имеет два формата:
- `DOC_OPEN` <dаtetime> <SEARCH_ID> <DOC_ID>
- `DOC_OPEN` <SEARCH_ID> <DOC_ID>

В выходных данных `result/opens.txt` для каждого открытия документа фиксируется дата в формате `dd.MM.yyyy`, извлекаемая из `DOC_OPEN`. Поэтому в случаях, когда DOC_OPEN в строке не имеет <dаtetime>, дата берется из соответствующего поиска с идентификатором `DOC_ID`.

## Логирование

Информация об ошибках и необработанных форматах собирается в `result/logs.txt`, туда же записываются данные при аварийной обработке сессии или события, когда получаем исключение - программа продолжает работу (обработку текущей сессии при исключении в событии), не учитывая сессию или событие - в логи попадают данные со stack trace

В логах предусмотрен аккумулятор ошибок и для каждого типа в файл выводится не больше заданного числа (10) записей

Добавим ошибок в один из файлов

После запуска в `result/logs.txt` появились логи с ошибками:
```
IllegalArgumentException in Some(scala.Predef$.require(Predef.scala:281)) -> 1
MatchError in Some(org.example.model.events.DocOpen$.parse(DocOpen.scala:29)) -> 1
Unknown event -> 1
----------
123 | Type: IllegalArgumentException
  Parsing event failed on: DOC_OPEN 01.06.202x_18:59:29 10156928 ACC_45614
  requirement failed: Not found DocOpen.datetime
  Stack trace:
    scala.Predef$.require(Predef.scala:281)
    at org.example.model.events.DocOpen.<init>(DocOpen.scala:13)
    at org.example.model.events.DocOpen$.parse(DocOpen.scala:46)

123 | Type: MatchError
  Parsing event failed on: DOC_OPEN 01.06.2020_18:59:30 10156928 LAW_367720 zzzzzzzz
  5 (of class java.lang.Integer)
  Stack trace:
    org.example.model.events.DocOpen$.parse(DocOpen.scala:29)
    at org.example.model.events.DocOpen$.parse(DocOpen.scala:21)
    at org.example.model.Session$.parse(Session.scala:68)

123 | Unknown event: START 01.06.2020_18:57:37


```

## Пример работы программы
```
(debug) QS total: 17137
(debug) CARD_SEARCH total: 4345
(Task1) ACC_45616 CARD_SEARCH count: 34
(Task2) Each document from QS for each day -> result/opens.txt
Error logs -> result/logs.txt

Process finished with exit code 0
```

## Тесты

Для парсера из `model/Session` написаны тесты c проверками на соответсвие с ожидаемыми количествами QS/CardSearch/DocOpen, которые также для файлов из `test/resources` построчно разбирают сессии, собирает объект domain и выводят его поля в `test/result`

Например для файла `test/resources/7247`:
```
SESSION_START 23.04.2020_21:20:23
CARD_SEARCH_START 23.04.2020_21:21:03
$134 инвентаризация тмц
CARD_SEARCH_END 
211758114 DOF_91786 DOF_100639 RZR_107970 DOF_100446 DOF_71360 DOF_100445 PBI_253397 DOF_71361 DOF_100435 PBI_270861 DOF_85886 PPN_49 DOF_71362 DOF_91794 PBI_199912 DOF_100451 DOF_87043 DOF_100457 DOF_91799 PKS_24 DOF_71359 DOF_100444 DOF_85869 DOF_91793 RZR_209425 RZR_27261 DOF_85885 DOF_91792 PBI_236817 PBI_259808 PKS_8 ACC_45614 DOF_85887 DOF_100618 DOF_100439 DOF_91782 DOF_85888 PPN_48 PBI_274177 DOF_91811 DOF_71350 DOF_71351 DOF_85870 DOF_85947 PBI_238262
DOC_OPEN  211758114 DOF_85886
DOC_OPEN  211758114 DOF_91792
DOC_OPEN  211758114 RZR_209425
DOC_OPEN  211758114 DOF_100457
DOC_OPEN  211758114 DOF_85870
DOC_OPEN  211758114 PBI_199912
CARD_SEARCH_START 23.04.2020_21:28:42
$134 ПБУ 10/99 п 172
$0 LAW_179199
CARD_SEARCH_END 
162106546 LAW_179199
DOC_OPEN  162106546 LAW_179199
SESSION_END 23.04.2020_21:32:19
```

получим файл `test/result/7247.txt`:
```
Session ID: 7247

Card Searches:
id: 211758114
datetime: 2020-04-23T21:21:03
params: (134, 'инвентаризация тмц')
foundDocs: DOF_91786, DOF_100639, RZR_107970, DOF_100446, DOF_71360, DOF_100445, PBI_253397, DOF_71361, DOF_100435, PBI_270861, DOF_85886, PPN_49, DOF_71362, DOF_91794, PBI_199912, DOF_100451, DOF_87043, DOF_100457, DOF_91799, PKS_24, DOF_71359, DOF_100444, DOF_85869, DOF_91793, RZR_209425, RZR_27261, DOF_85885, DOF_91792, PBI_236817, PBI_259808, PKS_8, ACC_45614, DOF_85887, DOF_100618, DOF_100439, DOF_91782, DOF_85888, PPN_48, PBI_274177, DOF_91811, DOF_71350, DOF_71351, DOF_85870, DOF_85947, PBI_238262
openDocs: DOF_85886, DOF_91792, RZR_209425, DOF_100457, DOF_85870, PBI_199912
---
id: 162106546
datetime: 2020-04-23T21:28:42
params: (134, 'ПБУ 10/99 п 172'), (0, 'LAW_179199')
foundDocs: LAW_179199
openDocs: LAW_179199
---
==================================================
No Query Searches found.
==================================================
Doc Opens:
datetime: 2020-04-23T21:21:03
searchId: 211758114
docId: DOF_85886
---
datetime: 2020-04-23T21:21:03
searchId: 211758114
docId: DOF_91792
---
datetime: 2020-04-23T21:21:03
searchId: 211758114
docId: RZR_209425
---
datetime: 2020-04-23T21:21:03
searchId: 211758114
docId: DOF_100457
---
datetime: 2020-04-23T21:21:03
searchId: 211758114
docId: DOF_85870
---
datetime: 2020-04-23T21:21:03
searchId: 211758114
docId: PBI_199912
---
datetime: 2020-04-23T21:28:42
searchId: 162106546
docId: LAW_179199
---

```
