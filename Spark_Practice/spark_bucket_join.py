import findspark
findspark.init()

import time
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from lib.logger import Log4J

spark = SparkSession.builder\
                    .master('local[3]')\
                    .appName('spark practice join')\
                    .enableHiveSupport()\ # spark DB에서는 필수
                    .getOrCreate()

logger = Log4J(spark)

flight_time_df1 = spark.read.json(os.getcwd()+r'\Spark_Practice\sample_data\d1') # 3partition
flight_time_df2 = spark.read.json(os.getcwd()+r'\Spark_Practice\sample_data\d2') # 3partition

df1 = flight_time_df1.alias('df1')
df2 = flight_time_df2.alias('df2')

# 수행했으니 주석표시..
"""
spark.sql("CREATE DATABASE IF NOT EXISTS test_DB")
spark.sql("USE test_DB")

# Do coalesce and wirte to DF to create and get a single partition
# next, use bucketBy (the 1st arg is the number of buckets, which means how many buckets I want to create)
# 3 Bucket Create -> Because, I'll run this application in three threads env
df1.coalesce(1).write\
                .bucketBy(3,"id")\
                .saveAsTable("test_DB.flight_data1")

df2.coalesce(1).write\
                .bucketBy(3,"id")\
                .saveAsTable("test_DB.flight_data2")
"""

# read df from database by using table() method
df3 = spark.read.table("test_DB.flight_data1")
df4 = spark.read.table("test_DB.flight_data2")

# do not able to automatically broadcast join
#(왜냐하면, 예제 DF는 너무 작기 때문에 자동적으로 broadcast join이 시전될 수 있기 때문임)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# join
join_expr = df3.id == df4.id
join_df = df3.join(df4, join_expr, 'inner')


join_df.show()
time.sleep(100)
