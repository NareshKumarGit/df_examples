from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import to_date,from_unixtime,unix_timestamp,avg,lag,row_number,dense_rank,rank,window
from src.model.Product import Product
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    finFilePath = "s3a://"+doc["s3_conf"]["s3_bucket"]+"/finances-small"
    financeDf = sparkSession.read.parquet(finFilePath)
    financeDf.printSchema()

    accNumPrev4WindowSpec = Window.partitionBy("AccountNumber")\
        .orderBy("Date")\
        .rowsBetween(-4, 0) #takes the first first 5 rows to window aggregation

    financeDf\
        .withColumn("Date", to_date(from_unixtime(unix_timestamp("Date", "MM/dd/yyyy"))))\
        .withColumn("RollingAvg", avg("Amount").over(accNumPrev4WindowSpec))\
        .show(5,False)

    productList= [
        Product("Thin", "Cell phone", 6000),
        Product("Normal", "Tablet", 1500),
        Product("Mini", "Tablet", 5500),
        Product("Ultra Thin", "Cell phone", 5000),
        Product("Very Thin", "Cell phone", 6000),
        Product("Big", "Tablet", 2500),
        Product("Bendable", "Cell phone", 3000),
        Product("Foldable", "Cell phone", 3000),
        Product("Pro", "Tablet", 4500),
        Product("Pro2", "Tablet", 6500)
    ]

    products = sparkSession.createDataFrame(productList)
    products.printSchema()

    catRevenueWindowSpec = Window.partitionBy("category")\
        .orderBy("revenue")

    products\
    .select("product",
            "category",
            "revenue",
            lag("revenue", 1).over(catRevenueWindowSpec).alias("prevRevenue"),
            lag("revenue", 2, "N/A").over(catRevenueWindowSpec).alias("prev2Revenue"),
            row_number().over(catRevenueWindowSpec).alias("row_number"),
            rank().over(catRevenueWindowSpec).alias("rev_rank"),
            dense_rank().over(catRevenueWindowSpec).alias("rev_dense_rank")
    )\
    .show()

"""
    financeDf\
        .select("*", window("Date", "30 days", "15 minutes"))\
        .show()
"""
