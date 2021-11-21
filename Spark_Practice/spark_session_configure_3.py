import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    spark_conf = get_spark_app_config() # sparkconf를 리턴하는 함수 호출
    spark = SparkSession.builder\
            .config(conf=spark_conf)\
            .getOrCreate()

    logger = Log4J(spark) # SparkSession 객체를 logger에 넘겨줌

    logger.info("Starting HelloSpark")

    conf_out = spark.sparkContext.getConf() # sparksession에 정의된 sparkconfig를 읽음
    logger.info(conf_out.toDebugString())
    logger.info("Finished HelloSpark")

    spark.stop()
