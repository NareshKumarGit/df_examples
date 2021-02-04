from pyspark.sql import SparkSession
from distutils.util import strtobool
import os.path
import yaml


if __name__=='__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"]=('--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell')

    spark=SparkSession \
        .builder \
        .appName("RDD Example") \
        .master("local[*]") \
        .getOrCreate()

    current_dir=os.path.abspath(os.path.dirname(__file__))
    appConfigpath=os.path.abspath(current_dir+"/../"+"application.yml")

    with open(appConfigpath) as conf:
        doc=yaml.load(conf,Loader=yaml.FullLoader)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    demographic_rdd= spark.sparkContext.textFile("s3a://" + doc["s3_conf"]["s3_bucket"] + "/demographic.csv")
    finances_rdd=spark.sparkContext.textFile("s3a://" + doc["s3_conf"]["s3_bucket"] + "/finances.csv")
    course_rdd=spark.sparkContext.textFile("s3a://" + doc["s3_conf"]["s3_bucket"] + "/course.csv")

    demographic_pair_rdd=demographic_rdd\
    .map(lambda line: line.split(","))\
    .map(lambda rec: (int(rec[0]),(int(rec[1]),strtobool(rec[2]),rec[3],rec[4], strtobool(rec[5]), strtobool(rec[6]),int(rec[7])))) \
    .filter(lambda lst: lst[1][2] == 'Switzerland')

    finances_pair_rdd=finances_rdd\
    .map(lambda line: line.split(","))\
    .map(lambda rec:(int(rec[0]),(strtobool(rec[1]),strtobool(rec[2]),strtobool(rec[3]),int(rec[4]))))\
    .filter(lambda rec: rec[1][1]==1 and rec[1][2]==1)


    course_rdd=course_rdd.map(lambda line:line.split(","))\
    .map(lambda rec: (int(rec[0]),rec[1]))
    join_rdd = demographic_pair_rdd.join(finances_pair_rdd)

    join_rdd=join_rdd.map(lambda rec: (int(rec[1][0][1]),(int(rec[0]),rec[1][0][2],rec[1][1][1],rec[1][1][2],rec[1][1][3])))

    join_course_rdd=join_rdd.join(course_rdd)
    join_course_rdd.foreach(print)



