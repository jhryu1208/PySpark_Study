import findspark
findspark.init()

import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import * # aggregation is all about using aggregate and windowing functions
from pyspark.sql.window import Window # Import Window
from lib.logger import Log4J

CURRENT_PATH = os.getcwd()
PATH = CURRENT_PATH + r'\Spark_Practice\sink_sample_data\groupbyexample\parquet_format'
spark = SparkSession.builder\
                    .master('local[3]')\
                    .appName('Spark Aggregation Pracice')\
                    .getOrCreate()

logger = Log4J(spark)

summary_DF = spark.read\
                .format('parquet')\
                .load(PATH+r'\part-00000-40c07c56-ab2b-443f-81a1-5fe6fcff8bf2-c000.snappy.parquet')

summary_DF.show()

# Define window : Partition & Ordering & Start/End
running_total_window = Window.partitionBy('Country')\
                             .orderBy('WeekNumber')\
                             .rowsBetween(Window.unboundedPreceding, Window.currentRow)

summary_DF.withColumn('RunningTotal',
                      sum("InvoiceValue").over(running_total_window)).show()
