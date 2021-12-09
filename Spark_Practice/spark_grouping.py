import findspark
findspark.init()

import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import * # aggregation is all about using aggregate and windowing functions
from lib.logger import Log4J

CURRENT_PATH = os.getcwd()
MIDDLE_PATH = r'\Spark_Practice\sample_data'
FILENAME = r'\invoices.csv'
PATH = CURRENT_PATH + MIDDLE_PATH + FILENAME
WRITE_PATH = CURRENT_PATH + r'\Spark_Practice\sink_sample_data\groupbyexample'
spark = SparkSession.builder\
                    .master('local[3]')\
                    .appName('Spark Aggregation Pracice')\
                    .getOrCreate()

logger = Log4J(spark)

invoices_DF = spark.read\
                    .format('csv')\
                    .option('header', 'true')\
                    .option('inferSchema', 'true')\
                    .load(PATH)

invoices_DF.show(10)
invoices_DF.printSchema()

invoices_DF = invoices_DF.withColumn('InvoiceDate', to_date(col('InvoiceDate'), 'dd-MM-yyyy H.mm'))\
                            .withColumn('WeekNumber', weekofyear(col('InvoiceDate')))\
                             .where(year(col('InvoiceDate')) == 2010)

result_DF = invoices_DF.groupBy('Country', 'WeekNumber').agg(
    countDistinct(col('InvoiceNo')).alias('NumInvoices'),
    sum(col('Quantity')).alias('TotalQuantity'),
    round(sum(col('Quantity')*col('UnitPrice')), 2).alias('InvoiceValue')
)

result_DF.sort('Country', 'WeekNumber').show()

# coalesce(1) : 분산된 상태로 저장하지 않기 위함
result_DF.coalesce(1)\
          .write\
          .format('csv')\
          .mode('overwrite')\
          .save(WRITE_PATH)
