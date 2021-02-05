from pyspark.sql import SparkSession
import yaml
import os.path

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../../" + "application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf, Loader=yaml.FullLoader)

    host = doc["mysql_conf"]["hostname"]
    port = doc["mysql_conf"]["port"]
    database = doc["mysql_conf"]["database"]
    jdbcParams = {"url": f"jdbc:mysql://{host}:{port}/{database}?autoReconnect=true&useSSL=false",
                  "lowerBound": "1",
                  "upperBound": "100",
                  "dbtable": doc["mysql_conf"]["dbtable"],
                  "numPartitions": "2",
                  "partitionColumn": doc["mysql_conf"]["partition_column"],
                  "user": doc["mysql_conf"]["username"],
                  "password": doc["mysql_conf"]["password"]
                  }
    print(jdbcParams)

    # use the ** operator/un-packer to treat a python dictionary as **kwargs
    print("\nReading data ingestion MySQL DB using SparkSession.read.format(),")
    txnDF = spark\
        .read.format("jdbc")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .options(**jdbcParams)\
        .load()

    txnDF.show()
