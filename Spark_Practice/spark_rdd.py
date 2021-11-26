import findspark
findspark.init()

import os
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import *
from collections import namedtuple
from lib.logger import Log4J
from lib.utils import get_spark_app_config

file_name = 'no_header_sample.csv'
current_path = os.getcwd()
middle_path = '\\Spark_Practice\\sample_data\\'
path = current_path+middle_path+file_name
SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])

# spark session & logger
spark_conf = get_spark_app_config()
spark = SparkSession.builder\
                    .config(conf = spark_conf)\
                    .getOrCreate()
sc = spark.sparkContext
logger = Log4J(spark)

# info : spark configuration
logger.info(print(sc))

linesRDD = sc.textFile(path)
partitionedRDD = linesRDD.repartition(2)

# ""를 제거해주지 않을 경우 다음과 같은 에러 발생
# TypeError: Can not infer schema for type: <class 'str'>
colsRDD = partitionedRDD.map(lambda line : line.replace('"', '').split(","))
colsRDD.toDF().show()

# 스키마 정보 지정 및 select
selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
selectRDD.toDF().show()

# filter
filteredRDD = selectRDD.filter(lambda rdd:rdd.Age < 40)
filteredRDD.toDF().show()

# key, value 지정
kvRDD = filteredRDD.map(lambda r: (r.Country, 1))
kvRDD.toDF().show()

# key, value 카운팅 by reduceByKey
countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)
countRDD.toDF().show()

colsList = countRDD.collect()

# collect 결과 출력
[logger.info(x) for x in colsList]
