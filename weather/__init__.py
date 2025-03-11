from re import match

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, desc, max, month, year

import os
import sys

from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Заполняем пропущенные числовые поля средними значениями по их полю
def fill_null_num_avg(dtfrm: DataFrame, header: StructType):
    for field in header:
        # Начиная с python 3.10
        # match field.dataType:
        #     case DoubleType:
        #         avg = dtfrm.select(avg(col(field.name))).collect()[0]
        #         df.na.fill(avg, subset=[field.name])
        if field.dataType == DoubleType():
            avg_v = df.select(avg(col(field.name))).collect()[0][0]
            dtfrm.na.fill(avg_v, subset=[field.name])



spark = SparkSession.builder \
    .appName('WeatherSpark') \
    .master('local[*]') \
    .getOrCreate()

# Явно указываем схему для наших данных
schema = StructType([
    StructField('station_id', StringType(), nullable=True),
    StructField('date', DateType(), nullable=True),
    StructField('temperature', DoubleType(), nullable=True),
    StructField('precipitation', DoubleType(), nullable=True),
    StructField('wind_speed', DoubleType(), nullable=True)
])

df = spark.read.csv('weather_data.csv',
                    header=True,
                    dateFormat='yyyy-MM-dd',
                    schema=schema)

df.printSchema()
# Не самый лучший способ узнать ВСЕ поля с Null
# df.filter(df.date.isNull()).show(5)
# df.filter(df.temperature.isNull()).show(5)
# df.filter(df.precipitation.isNull()).show(5)
# df.filter(df.wind_speed.isNull()).show(5)

# Чтобы оценить сколько полей имеют пропуски
df.summary().show(1)

df.select(col('temperature'))

fill_null_num_avg(df, schema)

print('Топ 5 жарких дней:')
df.select(['date','temperature']) \
    .orderBy(desc('temperature')) \
    .show(5)

print('Метеостанция с большим кол-вом осадков:')
df.groupby(col('station_id')) \
    .agg(
        max(col('precipitation')).alias('max_precipitation')
    ) \
    .orderBy(desc('max_precipitation')) \
    .show(1)

print('Средняя температура по месяцам:')
df.groupby([month('date').alias('month'), year('date').alias('year')]) \
    .agg(
        avg('temperature').alias('avg_temperature')
    ) \
    .orderBy(['year', 'month']) \
    .show()

spark.stop()