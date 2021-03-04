from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType, BooleanType,DoubleType
from pyspark.sql.functions import explode,col
import os.path
import yaml

if __name__=="__main__":

    os.environ["PYSPARK_SUBMIT_ARGS"]=(
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    spark=SparkSession\
    .builder\
    .appName("Dataframe")\
    .master("local[*]")\
    .getOrCreate()

    current_dir=os.path.abspath(os.path.dirname(__file__))
    appConf=os.path.abspath((current_dir+"/../"+"/application.yml"))
    secretConf=os.path.abspath((current_dir+"/../"+"/.secrets"))

    with open(appConf) as appconfig:
        doc=yaml.load(appconfig,Loader=yaml.FullLoader)

    with open(secretConf) as secretConfig:
        secretdoc=yaml.load(secretConfig,Loader=yaml.FullLoader)


    hadoop_conf=spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", secretdoc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", secretdoc["s3_conf"]["secret_access_key"])

    finSchema=StructType()\
    .add("id",IntegerType(),True)\
    .add("debt",BooleanType(),True)\
    .add("fiance_dependency",BooleanType(),True)\
    .add("loan",BooleanType(),True)\
    .add("income", DoubleType(),True)


    finDF=spark.read\
    .option("header","false")\
    .option("delimiter",",")\
    .format("csv")\
    .schema(finSchema)\
    .load("s3a://"+doc["s3_conf"]["s3_bucket"]+"/finances.csv")
    finDF.printSchema()
    finDF.show(5,False)


    financeDF=spark.read\
    .option("mode","DROPMALFORMED")\
    .option("header","false")\
    .option("delimiter",",")\
    .option("inferSchema","true")\
    .csv("s3a://"+doc["s3_conf"]["s3_bucket"]+"/finances.csv")\
    .toDF("id","debt","fiance_dependency","loan","income")

    financeDF.printSchema()
    financeDF.show(5,False)

    financeDF\
    .repartition(2)\
    .write\
    .partitionBy("id")\
    .mode("overwrite")\
    .option("header","true")\
    .option("delimiter","~")\
    .csv("s3a://"+doc["s3_conf"]["s3_bucket"]+"/fin")

    print("reading data from fin.csv after")
    findf=spark.read\
    .option("header","true")\
    .option("delimiter","~")\
    .format("csv")\
    .load("s3a://"+doc["s3_conf"]["s3_bucket"]+"/fin")

    findf.show(5,False)

    print("reading data from json company.json")
    jsondf=spark.read\
    .json("s3a://"+doc["s3_conf"]["s3_bucket"]+"/company.json")

    jsondf.show(5,False)

    flatterned_df=jsondf.select(col("company"),explode(col("employees")).alias("employee"))
    flatterned_df.show(5,False)

    flatterned_df.select(col("company"), col("employee.firstName").alias("name")).show(5)






