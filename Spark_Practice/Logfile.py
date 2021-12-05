import findspark
findspark.init()

from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from pyspark.sql import *
import os

spark = SparkSession.builder\
                    .master('local[3]')\
                    .appName('logfile_practice')\
                    .getOrCreate()
path = os.getcwd()+r'\Spark_Practice\sample_data\apache_logs.txt'
log_txt = spark.read.text(path)
log_txt.printSchema()
log_txt.show(1)
# 83.149.9.216 - - [17/May/2015:10:05:03 +0000] "GET /presentations/logstash-monitorama-2013/images/kibana-search.png HTTP/1.1" 200 203023 "http://semicomplete.com/presentations/logstash-monitorama-2013/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36"

log_reg = \
r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

log_df = log_txt.select(regexp_extract('value', log_reg, 1).alias('ip'),
                        regexp_extract('value', log_reg, 4).alias('date'),
                        regexp_extract('value', log_reg, 6).alias('request'),
                        regexp_extract('value', log_reg, 10).alias('referrer'))

log_df.show(10, truncate=False)

log_df.withColumn('referrer', substring_index('referrer', '/', 3))\
        .groupBy('referrer')\
        .count()\
        .show(10,truncate=False)
