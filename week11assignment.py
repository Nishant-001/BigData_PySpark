from pyspark.sql import SparkSession
from pyspark.sql import S
Logger.getLogger("org").setLevel(Level.ERROR)

sparkConf = newSparkConf()
sparkConf.set("spark.app.name", "My Application 1")
sparkConf.set("spark.master", "local[2]")

spark = SparkSession.builder()\
    .config(sparkConf)\
    .getOrCreate()


DDLString = "country String, weeknum Int, numinvoices Int, totalquantity Int, invoicevalue Double"

input = spark.read\
    .format("csv")\
    .schema(DDLString)\
    .option("path", "C:/Users/chnis/Downloads/Big Data/week11/windowdata.csv")\
    .load

input.write\
    .format("avro")\
    .partitionBy("country")\
    .mode("Overwrite")\
    .option("path", "C:/Users/chnis/Downloads/Big Data/week11/windowdata_output_avro")\
    .save()