from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4,org.apache.spark:spark-avro_2.11:2.4.5') \
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

    print("creating dataframe from parquet file")

    nyc_omo_df=spark.read\
    .parquet("s3a://" + doc["s3_conf"]["s3_bucket"] + "/NYC_OMO")\
    .repartition(5)

    print("# of records"+str(nyc_omo_df.count()))
    print("# get partition"+str(nyc_omo_df.rdd.getNumPartitions()))

    nyc_omo_df.printSchema()

    print("summary of nyc omo market order")
    nyc_omo_df.describe().show()

    print("frequency distribution of diff boroughs")
    nyc_omo_df\
    .groupBy("Boro")\
    .agg({"Boro":"count"})\
    .withColumnRenamed("count(Boro)","Order Frequency")\
    .show()

    print("OMO Zip and Borogh List")
    bro_zip_df=nyc_omo_df\
    .select("Boro", nyc_omo_df["Zip"].cast(IntegerType()))\
    .groupBy("Boro")\
    .agg({"Zip":"collect_set"})\
    .withColumnRenamed("collect_set(Zip)","ZipList")\
    .withColumn("Zipcount",F.size("ZipList"))

    bro_zip_df.show()

    bro_zip_df.select("Boro","Zipcount"," ZipList").show()

