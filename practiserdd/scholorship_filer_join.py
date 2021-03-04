from pyspark.sql import SparkSession
from distutils.util import strtobool
import os.path
import yaml

if __name__=="__main__":
    os.environ["PYSPARK_SUBMIT_ARGS"]=(
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    spark=SparkSession\
    .builder\
    .appName("RDD Sample")\
    .master('local[*]')\
    .getOrCreate()

    current_path=os.path.abspath(os.path.dirname(__file__))
    appConfig=os.path.abspath(current_path+"/../"+"application.yml")
    secretConfig=os.path.abspath(current_path+"/../"+".secrets")

    with open(appConfig) as appconf:
        doc=yaml.load(appconf,Loader=yaml.FullLoader)

    with open(secretConfig) as secconf:
        docsecret=yaml.load(secconf,Loader=yaml.FullLoader)

    hadoop_conf=spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key",docsecret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key",docsecret["s3_conf"]["secret_access_key"])

    demographic_rdd=spark.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/"+"demographic.csv")\
    .map(lambda rec: rec.split(","))\
    .map(lambda rec:(int(rec[0]),(int(rec[1]),strtobool(rec[2]),rec[3],rec[4],strtobool(rec[5]),strtobool(rec[6]),int(rec[7]))) )\
    .filter(lambda rec: rec[1][2]=="Switzerland")

    fin_rdd=spark.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/finances.csv")\
    .map(lambda line: line.split(","))\
    .map(lambda rec: (int(rec[0]),(strtobool(rec[1]),strtobool(rec[2]),strtobool(rec[3]),float(rec[4]))))\
    .filter(lambda rec: (rec[1][0]==1) and (rec[1][1]==1))

    join_pair_rdd= demographic_rdd.join(fin_rdd)

    course_rdd=spark.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/course.csv")\
    .map(lambda line: line.split(","))\
    .map(lambda rec: (int(rec[0]), (rec[1])))

    join_pair_rdd=join_pair_rdd.map(lambda rec: (rec[1][0][1], (rec[1][0][2], rec[1][1][0], rec[1][1][1])))\
    .join(course_rdd)



    join_pair_rdd.foreach(print)
