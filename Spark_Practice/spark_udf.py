import findspark
findspark.init()

import os
import re
from pyspark.sql import *
from pyspark.sql.functions import * # udf 포함
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from lib.logger import Log4J

def parse_gender(gender):
    """
    - ^f$, ^m$ : single character f, m
    - f.m, w.m, m.l : some character between first and third character
    """
    female_regexp = r'^f$|f.m|w.m'
    male_regexp = r'^m$|ma|m.l'

    if re.search(female_regexp, gender.lower()):
        return "Female"
    elif re.search(male_regexp, gender.lower()):
        return "Male"
    else:
        return "Unknown"

if __name__ == '__main__':
    spark = SparkSession.builder\
                        .master('local[3]')\
                        .appName('UDF_practice')\
                        .getOrCreate()

    logger = Log4J(spark)

    file_path = os.getcwd()+r'\Spark_Practice\sample_data\survey.csv'
    survey_DF = spark.read\
                        .option('header', 'true')\
                        .option('inferSchema', 'true')\
                        .format('csv')\
                        .load(file_path)

    survey_DF.show(10)

    # Column object expression
    parse_gender_udf = udf(parse_gender, StringType())
    logger.info("Catalog Entry:")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_DF2 = survey_DF.withColumn("Gender", parse_gender_udf('Gender'))
    survey_DF2.show(10)

    # SQL Expression
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    logger.info("Catalog Entry:")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_DF3 = survey_DF.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_DF3.show(10)
