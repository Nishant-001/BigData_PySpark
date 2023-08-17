from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()

rdd = spark.sparkContext.textFile("C:/Users/chnis/Desktop/test_data.txt")
rdd1 = rdd.map(lambda x: (x.split(":")[0], x.split(":")[1]))

rdd2 = rdd1.filter(lambda x: x[1] == 'spark')

ddlstring = "name string, word string"

wordschema = StructType([
    StructField("name", StringType()),
    StructField("word", StringType())
])
df = rdd1.toDF(ddlstring)

df1 = df.filter((df.word == "spark")).groupby("name")

df1.show()

# df.printSchema()
# rdd3 = rdd2.map(lambda x: (x[0], 1))
#
# rdd4 = rdd3.reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1])
# res = rdd4.collect()
#
# for i in res:
#     print(i)

