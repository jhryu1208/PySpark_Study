import findspark
findspark.init()

import os
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config


file_name = 'sample.csv'
current_path = os.getcwd()
middle_path = '\\Spark_Practice\\sample_data\\'
path = current_path+middle_path+file_name

spark_conf = get_spark_app_config()
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
sc = spark.sparkContext
logger = Log4J(spark)

survey_df = spark.read\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .csv(path)

print(type(survey_df))
survey_df.createOrReplaceTempView("survey_vw")

sql = """
SELECT country, count(*) AS cnt
FROM survey_vw
WHERE Age<40
GROUP BY country
"""
count_df = spark.sql(sql)
count_df.show()
