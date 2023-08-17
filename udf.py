from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
df = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("path", "C:/Users/chnis/Downloads/Big Data/week12 -Spark4(Structured Api 2)/dataset1") \
    .load()
df1 = df.toDF("name", "age", "city")


@udf(returnType=StringType())
def ageCheck(age):
    if age > 18:
        return "Y"
    else:
        return "N"


df2 = df1.withColumn("adult", ageCheck("age"))
df2.printSchema()
df2.show()
