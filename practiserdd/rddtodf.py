from pyspark.sql import SparkSession,Row
from distutils.util import strtobool
from pyspark.sql.types import StructType,StructField,IntegerType,BooleanType,StringType,DoubleType,FloatType
import os.path
import yaml

if __name__=="__main__":

    os.environ["PYSPARK_SUBMIT_ARGS"]=(
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    spark=SparkSession\
    .builder\
    .appName("RDD to DF")\
    .master("local[*]")\
    .getOrCreate()

    current_dir=os.path.abspath(os.path.dirname(__file__))
    appConf=os.path.abspath(current_dir+"/../"+"application.yml")
    secretConf = os.path.abspath(current_dir + "/../" + ".secrets")

    with open(appConf) as appConfig:
        doc=yaml.load(appConfig,Loader=yaml.FullLoader)

    with open(secretConf) as secretConfig:
        secretdoc=yaml.load(secretConfig,Loader=yaml.FullLoader)

    hadoop_config=spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_config.set("fs.s3a.access.key", secretdoc["s3_conf"]["access_key"])
    hadoop_config.set("fs.s3a.secret.key", secretdoc["s3_conf"]["secret_access_key"])

    txn_rdd= spark.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/txn_fct.csv")\
    .filter(lambda line: line.find("txn_id") )\
    .map(lambda line: line.split("|"))\
    .map(lambda rec: (int(rec[0]), rec[1], float(rec[2]),rec[3], rec[4], rec[5], rec[6]))

    for rec in txn_rdd.take(5):
        print(rec)

    txnWithNoColumn= txn_rdd.toDF()
    txnWithNoColumn.printSchema()
    txnWithNoColumn.show(5,False)

    txnWithNoColumn2=spark.createDataFrame(txn_rdd)
    txnWithNoColumn2.printSchema()
    txnWithNoColumn2.show(5, False)

    txnWithColumnName= txn_rdd.toDF(["txn_id","created_date","amount","customerid","status","merchantid","date"])
    txnWithColumnName.printSchema()
    txnWithColumnName.show(5,False)

