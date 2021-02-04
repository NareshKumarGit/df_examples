from pyspark.sql import SparkSession
from pyspark.sql.functions import first,trim,lower,ltrim,initcap,format_string,coalesce,lit,col
from src.model.Person import Person

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()

    peopleDf = sparkSession.createDataFrame([
        Person("Sidhartha", "Ray", 32, None, "Programmer"),
        Person("Pratik", "Solanki", 22, 176.7, None),
        Person("Ashok ", "Pradhan", 62, None, None),
        Person(" ashok", "Pradhan", 42, 125.3, "Chemical Engineer"),
        Person("Pratik", "Solanki", 22, 222.2, "Teacher")
    ])


    peopleDf.show()
    peopleDf.groupBy("firstName").agg(first("weightInLbs")).show()
    peopleDf.groupBy(trim(lower(col('firstName')))).agg(first("weightInLbs")).show()
    peopleDf.groupBy(trim(lower(col("firstName")))).agg(first("weightInLbs", True)).show()
    peopleDf.sort(col("weightInLbs").desc()).groupBy(trim(lower(col("firstName")))).agg(first("weightInLbs", True)).show()
    peopleDf.sort(col("weightInLbs").asc_nulls_last()).groupBy(trim(lower(col("firstName")))).agg(first("weightInLbs", True)).show()

    correctedPeopleDf = peopleDf\
        .withColumn("firstName", initcap("firstName"))\
        .withColumn("firstName", ltrim(initcap("firstName")))\
        .withColumn("firstName", trim(initcap("firstName")))\

    correctedPeopleDf.groupBy("firstName").agg(first("weightInLbs")).show()

    correctedPeopleDf = correctedPeopleDf\
        .withColumn("fullName", format_string("%s %s", "firstName", "lastName"))\

    correctedPeopleDf.show()

    correctedPeopleDf = correctedPeopleDf\
        .withColumn("weightInLbs", coalesce("weightInLbs", lit(0)))\

    correctedPeopleDf.show()

    correctedPeopleDf\
        .filter(lower(col("jobType")).contains("engineer"))\
        .show()

    # List
    correctedPeopleDf \
        .filter(lower(col("jobType")).isin(["chemical engineer","abc" ,"teacher"])) \
        .show()

    # Without List
    correctedPeopleDf\
        .filter(lower(col("jobType")).isin("chemical engineer", "teacher"))\
        .show()

    # Exclusion
    correctedPeopleDf \
        .filter(~lower(col("jobType")).isin("chemical engineer", "teacher")) \
        .show()

