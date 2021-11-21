import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J

if __name__ == "__main__":
    spark_conf = SparkConf()
    spark_conf.set("spark.app.name", "Hello Spark Conf")
    spark_conf.set("spark.master", "local[3]")
    spark = SparkSession.builder\
            .config(conf=spark_conf)\
            .getOrCreate()

    logger = Log4J(spark) # SparkSession 객체를 logger에 넘겨줌

    logger.info("Starting HelloSpark")
    logger.info("Finished HelloSpark")

    spark.stop()
