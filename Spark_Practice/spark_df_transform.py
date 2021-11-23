import findspark
findspark.init()

import os
from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J# 샘플 데이터의 data type guess
from lib.utils import get_spark_app_config, load_survey_df

if __name__ == "__main__":

    file_name = 'sample.csv'
    path = os.getcwd()+'\\Spark_Practice\\sample_data\\'+file_name

    spark_conf = get_spark_app_config() # sparkconf를 리턴하는 함수 호출
    spark = SparkSession.builder\
            .config(conf=spark_conf)\
            .getOrCreate()

    logger = Log4J(spark) # SparkSession 객체를 logger에 넘겨줌

    logger.info("Starting HelloSpark")
    survey_df = load_survey_df(spark, path)
    survey_df.show() # df 출력
    logger.info("Finished HelloSpark")

    spark.stop()
