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
