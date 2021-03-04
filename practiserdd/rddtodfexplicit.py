from pyspark.sql import  SparkSession, Row
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,LongType,FloatType,DoubleType,DateType,TimestampType
import os.path
import yaml
from pyspark.sql.functions import unix_timestamp,approx_count_distinct,sum

if __name__=="__main__":

    os.environ["PYSPARK_SUBMIT_ARGS"]=(
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    spark=SparkSession\
    .builder\
    .appName("RDD to DF")\
    .master("local[*]")\
    .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConf = os.path.abspath(current_dir + "/../" + "application.yml")
    secretConf = os.path.abspath(current_dir + "/../" + ".secrets")

    with open(appConf) as appConfig:
        doc = yaml.load(appConfig, Loader=yaml.FullLoader)

    with open(secretConf) as secretConfig:
        secretdoc = yaml.load(secretConfig, Loader=yaml.FullLoader)

    hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_config.set("fs.s3a.access.key", secretdoc["s3_conf"]["access_key"])
    hadoop_config.set("fs.s3a.secret.key", secretdoc["s3_conf"]["secret_access_key"])

    txn_rdd=spark.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/txn_fct.csv")\
    .filter(lambda line: line.find("txn_id"))\
    .map(lambda line: line.split("|"))\
    .map(lambda rec: Row( int(rec[0]), int(rec[1]), float(rec[2]),int(rec[3]), int(rec[4]), int(rec[5]), rec[6]))

    txn_fct_schema=StructType([
        StructField("txn_id",LongType(),False),
        StructField("created_date",LongType(),False),
        StructField("amount",DoubleType(),True),
        StructField("cust_id",LongType(),False),
        StructField("status", IntegerType(), True),
        StructField("mercahnt_id",LongType(),True),
        StructField("date",StringType(),True)
    ])

    txn_fct_df=spark.createDataFrame(txn_rdd,txn_fct_schema)
    txn_fct_df.printSchema()
    txn_fct_df.show(5,False)

    txn_fct_df=txn_fct_df.withColumn("date", unix_timestamp(txn_fct_df["date"], "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
    txn_fct_df.printSchema()
    txn_fct_df.show(5,False)

    print("# count of records "+str(txn_fct_df.count()))
    print("# distinct merchant is "+str(txn_fct_df.select(txn_fct_df["mercahnt_id"]).distinct().count()))
    print("# number of partitions "+str(txn_fct_df.rdd.getNumPartitions()))

    txn_fct_df=txn_fct_df\
    .repartition(10, txn_fct_df["mercahnt_id"])\
    .groupBy("mercahnt_id")\
    .agg(sum("amount"),approx_count_distinct("status"))
    txn_fct_df.show(5,False)

    txn_fct_df=txn_fct_df\
    .withColumnRenamed("sum(amount)","total_amount")\
    .withColumnRenamed("approx_count_distinct(status)","distinct_status_count")

    txn_fct_df.show(5,False)


