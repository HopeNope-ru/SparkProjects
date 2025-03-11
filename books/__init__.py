from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, year, desc

import os
import sys

from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName('BooksAndAuthor') \
    .master('local[*]') \
    .getOrCreate()

schema_author = StructType([
    StructField('author_id', IntegerType(), nullable=False),
    StructField('name', StringType(), nullable=True),
    StructField('birth_date', DateType(), nullable=True),
    StructField('country', StringType(), nullable=True)
])

df_authors = spark.read.csv('./authors.csv',
                           header=True,
                           dateFormat='yyyy-MM-dd',
                           schema=schema_author)

df_authors.printSchema()

schema_books = StructType([
    StructField('book_id', IntegerType(), nullable=False),
    StructField('title', StringType(), nullable=True),
    StructField('author_id', IntegerType(), nullable=True),
    StructField('genre', StringType(), nullable=True),
    StructField('price', DoubleType(), nullable=True),
    StructField('publish_date', DateType(), nullable=True)
])

df_books = spark.read.csv('./books.csv',
                          header=True,
                          dateFormat='yyyy-MM-dd',
                          schema=schema_books)

df_books.printSchema()

# Проверяем что поля не пустые
df_books.summary().show(1)
df_authors.summary().show(1)

# 1 Вариант соединения без дублирования поля
df_join = df_books.join(other=df_authors,
                        on=['author_id'],
                        how='inner')

# 2 Вариант соединения без дублирования поля
# df_join = df_books.join(other=df_authors,
#                         on=df_books.author_id == df_authors.author_id,
#                         how='inner').drop(df_authors.author_id)

df_join.show(5)

print('Топ 5 авторов, книги которых принесли прибыль:')
df_join.groupby([col('author_id'), col('name')]) \
    .agg(
        sum(col('price')).alias('total_revenue')
    ) \
    .orderBy(desc(col('total_revenue'))) \
    .show(5,truncate=False)

print('Кол-во книг в каждом жанре:')
df_join.groupby(col('genre')) \
    .agg(
        count(col('book_id')).alias('count_books')
    ) \
    .orderBy(col('count_books').desc) \
    .show()

print('Cредняя цена книг по каждому автору:')
df_join.groupby([col('author_id'), col('name')]) \
    .agg(
        avg(col('price')).alias('avg_price')
    ) \
    .orderBy(col('author_id').asc) \
    .show()

print('Книги опубликованные после 2000')
df_join.select([col('publish_date'), col('title'), col('price')]) \
    .filter(year(col('publish_date')) > 2000) \
    .orderBy(col('price').desc) \
    .show()