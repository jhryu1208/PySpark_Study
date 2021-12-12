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
                    .getOrCreate()

logger = Log4J(spark)

flight_time_df1 = spark.read.json(os.getcwd()+r'\Spark_Practice\sample_data\d1') # 3partition
flight_time_df2 = spark.read.json(os.getcwd()+r'\Spark_Practice\sample_data\d2') # 3partition

# This config will ensure that we get three partitions after the shuffle
# which means having three reduce exchanges
spark.conf.set("spark.sql.shuffle.partitions", 3)

join_expr = flight_time_df1.id == flight_time_df2.id
join_df = flight_time_df1.join(flight_time_df2, join_expr, 'inner')

# dummy action
join_df.foreach(lambda f: None)
input('hold spark web UI')
