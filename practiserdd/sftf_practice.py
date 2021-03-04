from pyspark.sql import SparkSession
import os.path
import yaml

if __name__=="__main__":

    spark=SparkSession\
    .builder\
    .appName("DF examples")\
    .master("local[*]")\
    .config("spark.jars.packages","com.springml:spark-sftp_2.11:1.1.1")\
    .getOrCreate()

    current_dir=os.path.abspath(os.path.dirname(__file__))
    app_config_path=os.path.abspath(current_dir+"/../"+"application.yml")
    secret_conf_path=os.path.abspath(current_dir+"/../"+".secrets")

    with open(app_config_path) as app_config:
        app_conf=yaml.load(app_config,Loader=yaml.FullLoader)

    with open(secret_conf_path) as secret_config:
        secret_conf=yaml.load(secret_config,Loader=yaml.FullLoader)


    oltxdf=spark.read\
        .format("com.springml.spark.sftp")\
        .option("host", secret_conf["sftp_conf"]["hostname"])\
        .option("port", secret_conf["sftp_conf"]["port"])\
        .option("username", secret_conf["sftp_conf"]["username"])\
        .option("pem", os.path.abspath(current_dir+"/../"+secret_conf["sftp_conf"]["pem"]))\
        .option("fileType","csv")\
        .option("delimiter","|")\
        .load(app_conf["sftp_conf"]["directory"]+"/receipts_delta_GBR_14_10_2017.csv")

    oltxdf.show(5)



