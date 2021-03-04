from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
import os.path
import yaml


if __name__=="__main__":

    os.environ["PYSPARK_SUBMIT_ARGS"]=(
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    spark= SparkSession\
    .builder\
    .appName("RDD files")\
    .master("local[*]")\
    .getOrCreate()

    current_dir=os.path.abspath((os.path.dirname(__file__)))
    app_config_path=os.path.abspath(current_dir+"/../"+"application.yml")
    secret_config_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf=open(app_config_path)
    app_conf=yaml.load(conf,Loader=yaml.FullLoader)

    secret=open(secret_config_path)
    conf_secret=yaml.load(secret,Loader=yaml.FullLoader)

    hadoop_conf=spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", conf_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", conf_secret["s3_conf"]["secret_access_key"])

    parque_df=spark.read\
    .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/NYC_OMO")\
    .repartition(5)

    parque_df.printSchema()
    parque_df.show(5,False)
    parque_df.describe().show()

    parque_df.groupBy("Boro")\
    .agg({"Boro":"count"})\
    .withColumnRenamed("count(Boro)" ,"Order_frequency")\
    .show()

    boro_zip_df=parque_df\
    .select("Boro",parque_df["Zip"].cast(IntegerType()))\
    .groupBy("Boro")\
    .agg({"Zip":"collect_set"})\
    .withColumnRenamed("collect_set(Zip)","ZipList")\
    .withColumn("ZipCount", F.size("ZipList"))

    boro_zip_df.select("Boro","ZipCount", "ZipList").show()

