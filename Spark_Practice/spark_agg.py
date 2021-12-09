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

# in aggregation, function does not count null
# Column Object
invoices_DF.select(count('*').alias('Count *'),\
                    sum('Quantity').alias('TotalQuantity'),\
                    avg('UnitPrice').alias('AvgPrice'),\
                    countDistinct("InvoiceNo").alias('countDistinct')).show()

# SQL Expression
invoices_DF.selectExpr(
    "count(1) as `count 1`",\
    "count(StockCode) as `count field`",\
    "sum(Quantity) as TotalQuantity",\
    "avg(UnitPrice) as AvgPrice"
).show()

# Summarize DataFrame : Column Object
invoices_DF.groupBy('Country', 'InvoiceNo').agg(
    sum('Quantity').alias('TotalQuantity'),
    round(sum(col('Quantity')*col('UnitPrice')), 2).alias('InvoiceValue')
    # or) expr("round(sum(Quantity*UnitPrice), 2) as InvoiceValue")
).show()

# Summarize DataFrame : SQL Expression
# Create View
invoices_DF.createOrReplaceTempView("sales")
spark.sql("""
    SELECT  Country, InvoiceNo,
            SUM(Quantity) AS TotalQuantity,
            ROUND(SUM(Quantity*UnitPrice), 2) AS InvoiceValue
    FROM sales
    GROUP BY Country, InvoiceNo
""").show()
