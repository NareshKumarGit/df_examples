from pyspark.sql import SparkSession, Row
from distutils.util import strtobool
from pyspark.sql.functions import unix_timestamp,approx_count_distinct,sum
from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,DoubleType,LongType,TimestampType,StringType
import os.path
import yaml

if __name__=='__main__':

      spark=SparkSession\
      .builder\
      .appName("RDD TODF External Schema")\
      .config('spark.jars.packages','org.apache.hadoop:hadoop-aws:2.7.4')\
      .master('local[*]')\
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

      txn_rdd=spark.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/txn_fct.csv")\
      .filter(lambda line:line.find("txn_id"))\
      .map(lambda line:line.split("|"))\
      .map(lambda rec:Row(int(rec[0]),int(rec[1]),float(rec[2]),int(rec[3]),int(rec[4]),int(rec[5]),rec[6]))

      txn_rdd_schema=StructType([
          StructField("txn_id", LongType(), False),
          StructField("created_time_str", LongType(), False),
          StructField("amount", DoubleType(), True),
          StructField("cust_id", LongType(), True),
          StructField("status", IntegerType(), True),
          StructField("merchant_id", IntegerType(), True),
          StructField("created_time_ist", StringType(), True)])


      txn_rdd_dfWithSchema=spark.createDataFrame(txn_rdd,txn_rdd_schema)
      txn_rdd_dfWithSchema.show(5,False)

      txn_rdd_dfWithSchema=txn_rdd_dfWithSchema\
      .withColumn("created_time_ist", unix_timestamp(txn_rdd_dfWithSchema["created_time_ist"], "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
      txn_rdd_dfWithSchema.printSchema()
      txn_rdd_dfWithSchema.show(5,False)

      print("total number of records "+str(txn_rdd_dfWithSchema.count()))
      print("total number of merchant"+str(txn_rdd_dfWithSchema.select(txn_rdd_dfWithSchema["merchant_id"]).distinct().count()))
      print("number of partitions ="+str(txn_rdd_dfWithSchema.rdd.getNumPartitions()))

      txn_aggr= txn_rdd_dfWithSchema\
      .repartition(10,txn_rdd_dfWithSchema["merchant_id"])\
      .groupBy("merchant_id")\
      .agg(sum("amount"),approx_count_distinct("status"))

      txn_aggr.show(5,False)

      txn_aggr=txn_aggr\
      .withColumnRenamed("sum(amount)","amount")\
      .withColumnRenamed("approx_count_distinct(status)","status renamed")
      txn_aggr.show(5,False)