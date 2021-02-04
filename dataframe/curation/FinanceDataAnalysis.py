from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,posexplode,expr,concat_ws,avg,sum,count,max,min,collect_set,size,sort_array,array_contains,when
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
    financeDf = sparkSession.sql("select * ingestion parquet.`{}`".format(finFilePath))

    financeDf.printSchema()
    financeDf.show(5,False)

    financeDf\
        .orderBy("Amount")\
        .show(5)

    #concat_ws function available sql.functions
    financeDf\
        .select(concat_ws(" - ", "AccountNumber", "Description").alias("AccountDetails"))\
        .show(5, False)

    financeDf\
        .withColumn("AccountDetails", concat_ws(" - ", "AccountNumber", "Description"))\
        .show(5, False)

    aggFinanceDf = financeDf\
        .groupBy("AccountNumber")\
        .agg(   avg("Amount").alias("AverageTransaction"),
                sum("Amount").alias("TotalTransaction"),
                count("Amount").alias("NumberOfTransaction"),
                max("Amount").alias("MaxTransaction"),
                min("Amount").alias("MinTransaction"),
                collect_set("Description").alias("UniqueTransactionDescriptions")
        )

    aggFinanceDf.show(5,False)

    aggFinanceDf\
        .select(
                        "AccountNumber",
                        "UniqueTransactionDescriptions",
                        size("UniqueTransactionDescriptions").alias("CountOfUniqueTransactionTypes"),
                        sort_array("UniqueTransactionDescriptions", False).alias("OrderedUniqueTransactionDescriptions"),
                        array_contains("UniqueTransactionDescriptions", "Movies").alias("WentToMovies")
        )\
        .show(5,False)

    companiesJson = [
    """{"company":"NewCo","employees":[{"firstName":"Sidhartha","lastName":"Ray"},{"firstName":"Pratik","lastName":"Solanki"}]}""",
    """{"company":"FamilyCo","employees":[{"firstName":"Jiten","lastName":"Pupta"},{"firstName":"Pallavi","lastName":"Gupta"}]}""",
    """{"company":"OldCo","employees":[{"firstName":"Vivek","lastName":"Garg"},{"firstName":"Nitin","lastName":"Gupta"}]}""",
    """{"company":"ClosedCo","employees":[]}"""
                    ]

    companiesRDD = sparkSession.sparkContext.parallelize(companiesJson)
    companiesDF = sparkSession.read.json(companiesRDD)

    companiesDF.show(5,False)
    companiesDF.printSchema()

    employeeDfTemp = companiesDF.select("company", explode("employees").alias("employee"))
    employeeDfTemp.show()
    employeeDfTemp2 = companiesDF.select("company", posexplode("employees").alias("employeePosition", "employee"))
    employeeDfTemp2.show()
    employeeDf = employeeDfTemp.select("company", expr("employee.firstName as firstName"))
    employeeDf.select("*",
            when(col("company") == "FamilyCo", "Premium")
            .when(col("company") == "OldCo", "Legacy")
            .otherwise("Standard").alias("Tier"))\
    .show(5,False)
