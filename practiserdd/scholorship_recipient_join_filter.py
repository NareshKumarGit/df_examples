from pyspark.sql import SparkSession
from distutils.util import strtobool
import os.path
import yaml

if __name__=="__main__":

    os.environ["PYSPARK_SUBMIT_ARGS"]=(
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell')

    #create sparksession
    spark=SparkSession\
    .builder\
    .appName("RDD SAMPLE")\
    .master('local[*]')\
    .getOrCreate()

    current_dir=os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath=os.path.abspath(current_dir+"/../"+"application.yml")
    secretConfigFilePath=os.path.abspath(current_dir+"/../"+".secrets")

    with open(appConfigFilePath) as appConf:
        doc=yaml.load(appConf,Loader=yaml.FullLoader)

    with open(secretConfigFilePath) as secretConf:
        docSecret=yaml.load(secretConf,Loader=yaml.FullLoader)


    hadoop_conf=spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key",docSecret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", docSecret["s3_conf"]["secret_access_key"])

    demo_rdd=spark.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/demographic.csv")
    fin_rdd=spark.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/finances.csv")

    demo_rdd_split=demo_rdd.map(lambda line: line.split(","))\
    .map(lambda rec:(int(rec[0]),(int(rec[1]), strtobool(rec[2]),rec[3],rec[4], strtobool(rec[5]), int(rec[7]))))
    fin_rdd_split=fin_rdd.map(lambda line: line.split(","))\
    .map(lambda rec: (int(rec[0]),(strtobool(rec[1]),strtobool(rec[2]),strtobool(rec[3]),float(rec[4]))))
    demo_rdd_split.foreach(print)
    fin_rdd_split.foreach(print)

    join_pair_rdd= demo_rdd_split\
    .join(fin_rdd_split)\
    .filter(lambda rec: (rec[1][0][2] == "Switzerland") and (rec[1][0][1]==1) and (rec[1][1][2]==1))

    join_pair_rdd.foreach(print)


