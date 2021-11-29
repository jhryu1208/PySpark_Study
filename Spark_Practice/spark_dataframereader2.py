import findspark
findspark.init()

import os
from pyspark.sql import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
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

# Spark DataFrame Schema  is a StructType
# StructField takes two mandatory arguments
# 1. colum nname
# 2. data type
df_schema_struct = StructType([
    StructField("FL_DATE", DateType()),
    StructField("OP_CARRIER", StringType()),
    StructField("OP_CARRIER_FL_NUM", IntegerType()),
    StructField("ORIGIN", StringType()),
    StructField("ORIGIN_CITY_NAME", StringType()),
    StructField("DEST", StringType()),
    StructField("DEST_CITY_NAME", StringType()),
    StructField("CRS_DEP_TIME", IntegerType()),
    StructField("DEP_TIME", IntegerType()),
    StructField("WHEELS_ON", IntegerType()),
    StructField("TAXI_IN", IntegerType()),
    StructField("CRS_ARR_TIME", IntegerType()),
    StructField("ARR_TIME", IntegerType()),
    StructField("CANCELLED", IntegerType()),
    StructField("DISTANCE", IntegerType()),
])

# CSV
csv_df = spark.read\
        .format('csv')\
        .option('header', 'true')\
        .schema(df_schema_struct)\
        .option('mode', 'FAILFAST')\
        .option('dateFormat', 'M/d/y')\
        .load(path+r'flight*.csv')

csv_df.show(5)
csv_df.select('FL_DATE', 'OP_CARRIER_FL_NUM', 'ORIGIN_CITY_NAME').printSchema()

# 더 직관적임
df_schema_ddl = \
"""
 FL_DATE DATE,
 OP_CARRIER STRING,
 OP_CARRIER_FL_NUM INT,
 ORIGIN STRING,
 ORIGIN_CITY_NAME STRING,
 DEST STRING,
 DEST_CITY_NAME STRING,
 CRS_DEP_TIME INT,
 DEP_TIME INT,
 WHEELS_ON INT,
 TAXI_IN INT,
 CRS_ARR_TIME INT,
 ARR_TIME INT,
 CANCELLED INT,
 DISTANCE INT
"""

# JSON
json_df = spark.read\
        .format('json')\
        .schema(df_schema_ddl)\
        .load(path+r'flight*.json')

json_df.show(5) 
json_df.select('FL_DATE', 'OP_CARRIER_FL_NUM', 'ORIGIN_CITY_NAME').printSchema()


# PARQUET
parquet_df = spark.read\
                .format('parquet')\
                .load(path+r'flight*.parquet')

parquet_df.show(5)
parquet_df.select('FL_DATE', 'OP_CARRIER_FL_NUM', 'ORIGIN_CITY_NAME').printSchema()
