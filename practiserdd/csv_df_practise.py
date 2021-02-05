from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,LongType,DoubleType, BooleanType
import yaml
import os.path

if __name__=='__main__':

    spark=SparkSession\
    .builder\
    .master('local[*]')\
    .appName("CSV DF")\
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4,org.apache.spark:spark-avro_2.11:2.4.5')\
    .getOrCreate()

    current_dir=os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath=os.path.abspath(current_dir+"/../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc=yaml.load(conf,Loader=yaml.FullLoader)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    finSchema = StructType() \
        .add("id", IntegerType(), True) \
        .add("has_debt", BooleanType(), True) \
        .add("has_financial_dependents", BooleanType(), True) \
        .add("has_student_loans", BooleanType(), True) \
        .add("income", DoubleType(), True)

    fct_df=spark.read\
    .option("header","false")\
    .option("delimiter",",")\
    .format("csv")\
    .schema(finSchema)\
    .load("s3a://"+doc["s3_conf"]["s3_bucket"]+"/finances.csv")

    fct_df.printSchema()
    fct_df.show()

    financeDf = spark.read \
        .option("mode", "DROPMALFORMED") \
        .option("header", "false") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .csv("s3a://" + doc["s3_conf"]["s3_bucket"] + "/finances.csv") \
        .toDF("id", "has_debt", "has_financial_dependents", "has_student_loans", "income")

    financeDf.show()

    financeDf\
    .repartition(2)\
    .write\
    .partitionBy("id")\
    .option("header", "true")\
    .option("delimiter", "~")\
    .csv("s3a://" + doc["s3_conf"]["s3_bucket"] + "/fin")



    financeDfwrite = spark.read \
        .option("mode", "DROPMALFORMED") \
        .option("header", "false") \
        .option("delimiter", "~") \
        .option("inferSchema", "true") \
        .csv("s3a://" + doc["s3_conf"]["s3_bucket"] + "/fin") \
        .toDF("id", "has_debt", "has_financial_dependents", "has_student_loans", "income")

    financeDfwrite.show()



    spark.stop()
