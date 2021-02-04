from pyspark.sql import SparkSession
import yaml
import os.path
from model.Student import Student

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.2" pyspark-shell'
    )

    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../" + "application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf, Loader=yaml.FullLoader)

    spark.conf.set("spark.mongodb.input.uri", doc["mongodb_config"]["input.uri"])
    spark.conf.set("spark.mongodb.input.uri", doc["mongodb_config"]["output.uri"])

    students = spark.createDataFrame(
    spark.sparkContext.parallelize(
        [Student("Sidhartha", "Ray", "ITER", 200), Student("Satabdi", "Ray", "CET", 100)]))

    students.show()

    students\
        .write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .mode("append")\
        .option("database", doc["mongodb_config"]["input.database"])\
        .option("collection", doc["mongodb_config"]["collection"])\
        .save()
