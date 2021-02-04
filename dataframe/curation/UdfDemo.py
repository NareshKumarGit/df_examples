from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,lit
from pyspark.sql.types import StringType

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()

    def capitalizeFirstUsingSpace(fullString: str):
        listString=fullString.split(" ")
        return ' '.join(list(map(str.upper,listString)))

    sampleDf = sparkSession\
        .createDataFrame(
            [(1, "This is some sample data"), (2, "and even more.")]
        ).toDF("id", "text")


    #Register: Method1
    capitalizerUDF1 = sparkSession.udf\
        .register("capitalizeFirstUsingSpace", capitalizeFirstUsingSpace,StringType())

    #Register: Method2
    capitalizerUDF2 = udf(capitalizeFirstUsingSpace, StringType())

    #Register: Method3
    capitalizerUDF3 = sparkSession.udf\
        .register("capitalizeFirstUsingSpace", lambda rec: ' '.join(list(map(str.upper,rec.split(" ")))), StringType())

    #Register: Method4
    capitalizerUDF4 = sparkSession.udf \
        .register("capitalizeFirstUsingSpace", lambda fullString,splitter: ' '.join(list(map(str.upper,fullString.split(splitter)))), StringType())

    sampleDf.select("id",
                    capitalizerUDF1("text").alias("text1"),
                    capitalizerUDF2("text").alias("text2"),
                    capitalizerUDF3("text").alias("text3"),
                    capitalizerUDF4("text",lit(" ")).alias("text4")
                    ).show(5,False)

