from pyspark.sql import SparkSession
import yaml
import os.path
import sys
import utils.aws_utils as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar"\
         --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf, Loader=yaml.FullLoader)

    # Read access key and secret key ingestion cmd line argument
    s3_access_key = doc["s3_conf"]["access_key"]
    s3_secret_access_key = doc["s3_conf"]["secret_access_key"]
    if len(sys.argv) > 1 and sys.argv[1] is not None and sys.argv[2] is not None:
        s3_access_key = sys.argv[1]
        s3_secret_access_key = sys.argv[2]

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", s3_access_key)
    hadoop_conf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    print("Reading txn_fact table ingestion AWS Redshift and creating Dataframe,")

    jdbcUrl = ut.get_redshift_jdbc_url(doc)
    print(jdbcUrl)
    txnDf = spark.read\
        .format("io.github.spark_redshift_community.spark.redshift")\
        .option("url", jdbcUrl) \
        .option("dbtable", "PUBLIC.TXN_FCT") \
        .option("forward_spark_s3_credentials", "true")\
        .option("tempdir", "s3a://"+doc["s3_conf"]["s3_bucket"]+"/temp")\
        .load()

    txnDf.show(5, False)

