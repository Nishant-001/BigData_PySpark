from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

sparkConf = SparkConf()
sparkConf.set("spark.app.name", "My Application 1")
sparkConf.set("spark.master", "local[2]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()
ordersSchemaDDL = "orderid Int, orderdate String, customerid Int,status String"
ordersSchema = StructType([
    StructField("orderid", IntegerType()),
    StructField("orderdate", TimestampType()),
    StructField("customerid", IntegerType()),
    StructField("status", StringType())
])
ordersDf = spark.read.format("csv") \
    .option("header", True) \
    .schema(ordersSchemaDDL) \
    .option("path", "C:/Users/chnis/Downloads/Big Data/week11/orders.csv") \
    .load()

gropuedOrdersDf = ordersDf.repartition(4).where("orderid >10000").select("orderid", "customerid").groupBy(
    "orderid").count()

gropuedOrdersDf.show()

ordersDf.printSchema()

spark.stop()
