import findspark
findspark.init()

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder\
                    .master('local[3]')\
                    .appName('Spark Transformation Practice')\
                    .getOrCreate()

"""
createDataFrame()
"""
data_list = [
                ("Ravi", 28, 1, 2002),
                ("Abdul", 23, 5, 81),
                ("John", 12, 12, 6),
                ("Rosy", 7, 8, 63),
                ("Abdul", 23, 5, 81)
            ]

raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year")
raw_df.printSchema()


"""
monotonically_increasing_id()
"""
raw_df = raw_df.repartition(3)

df1 = raw_df.withColumn("id", monotonically_increasing_id())
df1.show()


"""
cast and case when statement
"""
casequery = """
CASE
    WHEN year < 21 THEN year + 2000
    WHEN year < 100 THEN year + 1900
    ELSE year
END
"""
df2 = df1.withColumn("year", expr(casequery).cast('Integer'))
df2.show()

df2 = df1.withColumn("year", when(col('year')<21, col('year')+2000)\
															.when(col('year')<100, col('year')+ 1900)\
																.otherwise(col('year')))
df2.show()

"""
add/delete column
"""
df3 = df2.withColumn("dob1", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')"))
df4 = df3.withColumn("dob2", to_date(expr("concat(day, '/', month, '/', year)"), 'd/M/y'))\
            .drop('day', 'month', 'year')
df4.show()

"""
duplicate
"""
df5 = df4.dropDuplicates(['name', 'dob1'])
df5.show()

"""
sort
"""
df6 = df5.sort(expr("dob1 desc"))
df6.show()
