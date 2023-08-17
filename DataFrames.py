from pyspark import SparkConf
from pyspark.sql import SparkSession
sparkConf = SparkConf()
sparkConf.set("spark.app.name", "My Application 1")
sparkConf.set("spark.master", "local[2]")

spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

ordersDf = spark.read.option("header", True).option("inferSchema", True).csv("C:/Users/chnis/Downloads/Big Data/week11/orders.csv")


gropuedOrdersDf = ordersDf.repartition(4).where("order_customer_id >10000").select("order_id", "order_customer_id").groupBy("order_customer_id").count()

gropuedOrdersDf.show()

ordersDf.printSchema()

spark.stop()
