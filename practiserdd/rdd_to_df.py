from pyspark.sql import SparkSession
from distutils.util import strtobool
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .master("local[*]")\
        .appName("RDD examples") \
        .master('local[*]') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../" + "application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")


    txn_fct_rdd = spark.sparkContext.textFile("s3a://" + doc["s3_conf"]["s3_bucket"] + "/txn_fct.csv")\
    .filter(lambda rec: rec.find("txn_id"))\
    .map(lambda rec:rec.split("|"))\
    .map(lambda rec:(int(rec[0]),rec[1],float(rec[2]),rec[3],rec[4],rec[5],rec[6]))

    for txn in txn_fct_rdd.take(5):
        print(txn)
    print("Printing RDD to DF using toDF() Function")
    txnDFNoColNames=txn_fct_rdd.toDF()
    txnDFNoColNames.printSchema()
    txnDFNoColNames.show(5,False)

    print("Printing RDD to DF using createdataframe() Function")
    txnDfNoColName2=spark.createDataFrame(txn_fct_rdd)
    txnDfNoColName2.printSchema()
    txnDfNoColName2.show(5,False)

    print("Dataframe with column name:String")
    txnDFWithColNames = txn_fct_rdd.toDF(["txn_id","create_time","Amount","cust_id","status","created_time"])
    txnDFWithColNames.printSchema()
    txnDFWithColNames.show(5,False)



