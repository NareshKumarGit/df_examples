from pyspark.sql import SparkSession
import os.path
import yaml

if __name__=="__main__":
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    spark=SparkSession\
    .builder\
    .appName("Dataframe")\
    .master("local[*]")\
    .getOrCreate()

    current_dir=os.path.abspath(os.path.dirname(__file__))
    app_conf_path=os.path.abspath(current_dir+"/../"+"application.yml")
    secret_conf_path=os.path.abspath(current_dir+"/../"+".secrets")

    conf=open(app_conf_path)
    app_conf=yaml.load(conf,Loader=yaml.FullLoader)
    secret=open(secret_conf_path)
    app_secret=yaml.load(secret,Loader=yaml.FullLoader)

    host = app_secret["mysql_conf"]["hostname"]
    port = app_secret["mysql_conf"]["port"]
    database = app_secret["mysql_conf"]["database"]
    url="jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)

    jdbcParams = {"url": url,
                  "lowerBound": "1",
                  "upperBound": "100",
                  "dbtable": app_conf["mysql_conf"]["dbtable"],
                  "numPartitions": "2",
                  "partitionColumn": app_conf["mysql_conf"]["partition_column"],
                  "user": app_secret["mysql_conf"]["username"],
                  "password": app_secret["mysql_conf"]["password"]
                  }
    print("\nReading data from MySQL DB using SparkSession.read.format(),")
    txnDF = spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbcParams) \
        .load()

    txnDF.show()


