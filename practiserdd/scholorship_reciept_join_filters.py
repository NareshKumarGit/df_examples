from pyspark.sql import SparkSession
from distutils.util import strtobool
import os.path
import yaml

if __name__=='__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"]=('--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell')

    spark=SparkSession\
        .builder\
        .appName("RDD Example")\
        .master("local[*]")\
        .getOrCreate()

    current_dir=os.path.abspath(os.path.dirname(__file__))
    appConfigPath=os.path.abspath(current_dir+"/../"+"application.yml")

    with open(appConfigPath) as conf:
        doc= yaml.load(conf,Loader=yaml.FullLoader)

    hadoop_conf=spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint","s3-eu-west-1.amazonaws.com")

    demographics_rdd= spark.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/demographic.csv")
    finances_rdd=spark.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/finances.csv")
    demographic_pair_rdd=demographics_rdd.map(lambda line:line.split(','))\
    .map(lambda rec:(int(rec[0]),(int(rec[1]), strtobool(rec[2]),rec[3],rec[4],strtobool(rec[5]),strtobool(rec[6]),int(rec[7]))))
    finances_pair_rdd=finances_rdd\
    .map(lambda line: line.split(","))\
    .map(lambda rec: (int(rec[0]), (strtobool(rec[1]), strtobool(rec[2]),strtobool(rec[3]),int(rec[4]) ) ))

    join_pair_rdd=demographic_pair_rdd.join(finances_pair_rdd)
    join_pair_rdd=join_pair_rdd.filter(lambda rec: rec[1][0][2]=='Switzerland' and rec[1][1][1]==1 and rec[1][1][2]==1)
    join_pair_rdd.foreach(print)
