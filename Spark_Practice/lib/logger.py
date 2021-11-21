class Log4J:
    # spark인자는 SparkSession Object를 의미한다.
    def __init__(self, spark):
            # SparkSession의 JVM으로 부터 log4j 인스턴스를 받아온다.
            # message를 로깅하기 위한 logger attribute 생성
            # log4j properties파일에서 지정한 logger name을 targeting한다.
            # 더 나아가 appname(builder의 appName메소드)을 SparkSession Conf에서 가져와 추가할 수 있다.
            root_class = "guru.learningjournal.spark.examples"
            conf = spark.sparkContext.getConf()
            app_name = conf.get("spark.app.name")
            log4j = spark._jvm.org.apache.log4j
            self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)


    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)
