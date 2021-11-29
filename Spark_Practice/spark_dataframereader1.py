import findspark
findspark.init()

import os
from pyspark.sql import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from lib.logger import Log4J
from lib.utils import get_spark_app_config

init_path = os.getcwd()
middle_path = r'/Spark_Practice/sample_data/flight-time/'
path = init_path + middle_path

spark_conf = get_spark_app_config()
spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
sc = spark.sparkContext

logger = Log4J(spark)

logger.info(f'Spark Application Id : {spark._sc.applicationId}')

csv_df = spark.read\
        .format('csv')\
        .option('header', 'true')\
        .option('inferSchema', 'true')\
        .load(path+r'flight*.csv')

csv_df.show(5)
csv_df.select('FL_DATE', 'OP_CARRIER_FL_NUM', 'ORIGIN_CITY_NAME').printSchema()

# [ csv result : infer shema option O ]
# root
# |-- FL_DATE: string (nullable = true)
# |-- OP_CARRIER_FL_NUM: string (nullable = true)
# |-- ORIGIN_CITY_NAME: string (nullable = true)
# ∴ 각 필드의 데이터 타입이 제대로 파싱되지 않음 확인
# [ csv result : infer shema option X ]
#root
# |-- FL_DATE: string (nullable = true)
# |-- OP_CARRIER_FL_NUM: integer (nullable = true)
# |-- ORIGIN_CITY_NAME: string (nullable = true)

json_df = spark.read\
        .format('json')\
        .load(path+r'flight*.json')

json_df.show(5) # JSON format also sorted the column names in alphabetical order.
json_df.select('FL_DATE', 'OP_CARRIER_FL_NUM', 'ORIGIN_CITY_NAME').printSchema()

# [ json result ]
# root
# |-- FL_DATE: string (nullable = true)
# |-- OP_CARRIER_FL_NUM: long (nullable = true)
# |-- ORIGIN_CITY_NAME: string (nullable = true)


parquet_df = spark.read\
                .format('parquet')\
                .load(path+r'flight*.parquet')

parquet_df.show(5)
parquet_df.select('FL_DATE', 'OP_CARRIER_FL_NUM', 'ORIGIN_CITY_NAME').printSchema()

# [ parquet result ]
# root
# |-- FL_DATE: date (nullable = true)
# |-- OP_CARRIER_FL_NUM: integer (nullable = true)
# |-- ORIGIN_CITY_NAME: string (nullable = true)
