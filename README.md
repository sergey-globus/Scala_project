# SessionMapReduceApp

## Используемые версии зависимостей и плагинов

- Scala version: **2.12.18**  
- Spark version: **3.5.0**  
- Java compiler source/target: **11**

## Сборка и запуск

Для сборки проекта (JAR-файла с контейнером всех зависимостей) используется Maven:
```
mvn clean compile
```

После этого JAR-файл будет находиться в директории `target`, обычно с именем вида `spark-project-1.0-SNAPSHOT-jar-with-dependencies.jar`.

Для запуска приложения используйте команду `spark-submit`:

```
spark-submit --class org.example.SessionMapReduceApp --master local[*] target/spark-project-1.0-SNAPSHOT-jar-with-dependencies.jar [input_path] [output_path]
```
