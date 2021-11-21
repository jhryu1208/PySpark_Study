from pyspark.sql import *
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder\
            .appName("HelloSpark")\
            .master("local[3]")\
            .getOrCreate()

    logger = Log4J(spark) # SparkSession 객체를 logger에 넘겨줌

    logger.info("Starting HelloSpark")
    logger.info("Finished HelloSpark")

    spark.stop()
