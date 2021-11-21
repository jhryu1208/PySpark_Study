import findspark
findspark.init()

import os
import configparser
from pyspark import SparkConf

# this func will load the configurations from spark.conf file
# and return SparkConf object
def get_spark_app_config():

    file_name = 'test_spark.conf'
    path = os.getcwd() + '\\Spark_Practice\\conf\\'+file_name # 파일의 절대경로 path
    spark_conf = SparkConf() # SparkConf객체 생성
    config = configparser.ConfigParser() # ConfigParser객체 생성
    config.read(path) # path로 부터 파일 읽어오기

    for (key, val) in config.items("SPARK_APP_LOCAL_CONFIGS"):
        spark_conf.set(key, val) # spark_conf set

    return spark_conf

# load dataframe function
def load_survey_df(spark, data_file):
    # read 메소드 : DataFrameReader 객체 반환, DataFrameReader -> GateWay to read data in Spark
    # option 메소드 : spark documentation으로 부터 필요한 csv리스트를 얻을 수 있음, key-value 페어 기입, header를 가진 파일이기 때문에 header옵션 사용
    # option 메소드의 inferSchema : 샘플 데이터의 data type guess
    df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(data_file)

    return df
