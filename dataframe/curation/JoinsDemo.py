from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from src.model.Role import Role
from src.model.Employee import Employee

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()


    employeeDf = sparkSession.createDataFrame([
        Employee(1,"Sidhartha", "Ray"),
        Employee(2, "Pratik", "Solanki"),
        Employee(3, "Ashok", "Pradhan"),
        Employee(4, "Rohit", "Bhangur"),
        Employee(5, "Kranti", "Meshram"),
        Employee(7, "Ravi", "Kiran")
    ])

    empRoleDf = sparkSession.createDataFrame([
                                                Role(1, "Architect"),
                                                Role(2, "Programmer"),
                                                Role(3, "Analyst"),
                                                Role(4, "Programmer"),
                                                Role(5, "Architect"),
                                                Role(6, "CEO")
                                            ])

    #employeeDf.join(empRoleDf, "id" === "id").show(false)   #Ambiguous column name "id"
    employeeDf.join(empRoleDf,employeeDf.id == empRoleDf.id).show(5,False)

    employeeDf.join(broadcast(empRoleDf),employeeDf["id"] == empRoleDf["id"]).show(5,False)
    #Join Types: "left_outer"/"left", "full_outer"/"full"/"outer"
    employeeDf.join(empRoleDf, [employeeDf["id"] == empRoleDf["id"]], "inner").show()
    employeeDf.join(empRoleDf, [employeeDf["id"] == empRoleDf["id"]], "right_outer").show()
    employeeDf.join(empRoleDf, [employeeDf["id"] == empRoleDf["id"]], "left_anti").show()
    employeeDf.join(empRoleDf, [employeeDf["id"] == empRoleDf["id"]], "full").show()


    # cross join
    employeeDf.join(empRoleDf, [employeeDf["id"] == empRoleDf["id"]], "cross").show()

