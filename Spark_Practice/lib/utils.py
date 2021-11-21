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
