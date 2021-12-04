import findspark
findspark.init()

import os
from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id
from lib.logger import Log4J

spark = SparkSession.builder\
        .master('local[3]')\
        .appName('Spark_DataframeWriter_Practice')\
        .getOrCreate()

logger = Log4J(spark)
current_path = os.getcwd()

# read_path
read_middle_path = r'\Spark_Practice\sample_data\flight-time'
read_file_name = r'\flight*.parquet'
read_path = current_path + read_middle_path + read_file_name

# write_path
write_middle_path = r'\Spark_Practice\sink_sample_data'
write_path = current_path+ write_middle_path

read_DF = spark.read\
                .format('parquet')\
                .load(read_path)


read_DF.write\
        .format('csv')\
        .mode('overwrite')\
        .option("path", write_path+r'\default')\
        .save()

logger.info('Num Partition Before : ' + str(read_DF.rdd.getNumPartitions())) # read한 파일의 파티션 수
read_DF.groupBy(spark_partition_id()).count().show()


# repartition
repartition_DF = read_DF.repartition(5)

repartition_DF.write\
                .format('csv')\
                .mode('overwrite')\
                .option('path', write_path+r'\repartition=5')\
                .save()

logger.info('Num partition Before : ' + str(repartition_DF.rdd.getNumPartitions()))
repartition_DF.groupBy(spark_partition_id()).count().show()


# repartitionby 2 columns
read_DF.write\
        .format('csv')\
        .mode('overwrite')\
        .option('path', write_path+r'\partitionBy')\
        .partitionBy('OP_CARRIER','ORIGIN')\
        .save()

# partition file size control
# file당 10k레코드로 제한
read_DF.write\
        .format('csv')\
        .mode('overwrite')\
        .option('path', write_path+r'\maxRecordsPerfile')\
        .partitionBy('OP_CARRIER','ORIGIN')\
        .option('maxRecordsPerfile', 10000)\
        .save()
