import findspark
findspark.init()

import os
from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J# 샘플 데이터의 data type guess
from lib.utils import get_spark_app_config, load_survey_df, count_by_country
import time

if __name__ == "__main__":

    file_name = 'sample.csv'
    path = os.getcwd()+'\\Spark_Practice\\sample_data\\'+file_name

    spark_conf = get_spark_app_config() # sparkconf를 리턴하는 함수 호출
    spark = SparkSession.builder\
            .config(conf=spark_conf)\
            .getOrCreate()
    print(f'application_id : {spark._sc.applicationId}')

    logger = Log4J(spark) # SparkSession 객체를 logger에 넘겨줌

    logger.info("Starting HelloSpark")

    survey_df = load_survey_df(spark, path)
    # 로컬 파일을 이용하기 때문에, 로드된 파일은 파티션이 되어있지 않기 때문에 임의로 파티션을 진행한다.
    partitioned_survey_df = survey_df.repartition(2)

    # Transform
    # count_by_country함수에는 wide 종속성인 groupby가 존재한다.
    # 그런데, wide 종속성 operation으로 인해서 몇 개의 파티션이 나올지 알 수 없다.
    # 만약, 이 출력되는 파티션의 조절하고 싶을 경우 configuration을 통해서 다음의 옵션을 추가한다.
    # spark.sql.shuffle.partitions = 2 (shuffle&sort로 인해 결과로 출력되는 파티션의 수는 2개로 제한한다는 의미)
    # (*현재 코드는 config파일을 수정하고 진행하는 코드임을 메모한다.)
    count_df = count_by_country(partitioned_survey_df)

    # show말고 collect를 사용하는 이유?
    # 1. collect action returns "the df" as python list, 반면 show는 debugging용도임
    # 2. show method는 complex internal code로 코드를 컴파일함 -> 복잡한 topic을 다룰 때는 불필요한 혼동을 야기할 수 있음
    logger.info(count_df.collect())

    # Spark Web Ui는 코드가 실행 중인 동안에만 확인할 수 있다.
    # 따라서, 이런 짧은 코드를 Web Ui에서 확인하고 싶은 경우 다음과 같은 input으로 프로그램이 끝나지 않도록 할 수 있다.
    # 그리고, 그 동안에 web ui를 확인할 수 있다.
    # 하지만, local debugging할 때만 사용하고 그 외에는 반드시 삭제하자!
    time.sleep(600)
    logger.info("Finished HelloSpark")

    spark.stop()
