# Spark Educational Projects
## Weather
### Что узнал нового
#### Работа с чтением в DataFrame
- Указав в чтении csv файла с датами в DataFrame с автоматическим определением схемы(inferSchema=True)[spark.read.csv('name.csv', head=True, inferSchema=True)]. Заметим, что наши даты без времени автоматически переведутся в тип данных TimeStamp.
- Чтобы перевести в дату без времени из TimeStamp создаем самостоятельно схему(schema) и передаем ее в чтение csv файла
```python
from pyspark.sql.types import StructType, StructField, DateType

schema = StructType([
    # ...
    StructField('date', DateType(), nullable=True),
    #   ...
])
```
- <b>Явно</b> указываем формата даты в DataFrame при чтении файла из csv указываем опцию dateFormat='yyyy-MM-dd'.
```python
df = spark.read.csv('path/file.csv', header=True, dateFormat='yyyy-MM-dd', schema=schema)
```
#### Обработка DataFrame
- Заполнение нулевых полей значением fill(value:long) от DataFrameNaFunctions (https://sparkbyexamples.com/pyspark/pyspark-fillna-fill-replace-null-values/)
```python
#Заменим 0 для всех null в колонках целого типа
df.na.fill(value=0).show()

# Заменим 0 для null только в выбранном поле 
df.na.fill(value=0,subset=["population"]).show()
```
- Расчет среднего значения(pyspark.sql.functions avg) по колонке (pyspark.sql.functions col)
- Отфильтровываем строки по null (https://sparkbyexamples.com/pyspark/pyspark-filter-rows-with-null-values/)
- При группировки и агрегировании полей не обязательно указывать в запросе Spark API select([поля]). Сам Spark приводит таблицу к нужному ввиду. Это можно увидеть в листинге 63 строки.
- orderBy тоже самое что и sort в PySpark. В самом исходном коде orderBy = sort
- Для оценки пустых полей в строках используем конструкцию df.summary().show(1)
- filter() похож на предикат where 

## Books
### Что узнал нового

* Ошибка - Reference <название поля> is ambiguous, could be: [<название поля>, <название поля>]. Эта ошибка возникает когда мы соединяем DataFrame-ы с одинаковым названием поля. Чтобы ее устранить можно воспользоваться следующими техниками:
  * В join указываем в списке одно поле, по которому идет соединение
      ```python
      df.join(other=df_other, on=['the_same_field'], how='inner')
      ```
  * Использовать drop(<df.название поля>) или явно в select() указать все поля
      ```python
          df.join(other=df_other, on=df.field == df_other.field, how='inner')
            .drop(df_other.field)
      ```
    
* Можно использовать функции Column для сортировки, чтобы не городить большой огород как в примере:
```python
df.orderBy(col('name_column').desc) # выглядит уже прилично и понятно читается
df.orderBy(desc(col('name_column'))) # большой огород
```

* часть библиотеки pyspark.sql.functions похожа с теми же функциями, которые имеет и обычный SQL. Например avg, sum, count, desc, asc, year, month, day, to_date и т.д.

## Best Практики

-  Перед обработкой данных лучше иметь DataFrame без null строк или значений. Для этого необходимо все очистить. Как это было в задании Weather.

## Что стоит изучить

- match в Python начиная с 3.10. Как с ним работать?