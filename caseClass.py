from pyspark.sql import SparkSession
from pyspark import SparkConf
from py4j import
Case class OrdersData(order_id: Int, order_date: TimeStamp, order_customer_id: Int, order_status: String)
Logger.getLogger("org").setLevel(Level.ERROR)

sparkConf = SparkConf()
sparkConf.set("spark.app.name", "My Application 1")
sparkConf.set("spark.master", "local[2]")

spark = SparkSession.builder()\
    .config(sparkConf)\
    .getOrCreate()

# /
# ordersSchema = StructType(List( // Programatic
# way
# // StructField("orderid", IntegerType, true),
# // StructField("orderdate", TimestampType),
# // StructField("customerid", IntegerType),
# // StructField("status", StringType)
#    //      ))


ordersSchemaDDL = "orderid Int, orderdate String, custid Int,ordersStatus String"

ordersDf = spark.read\
    .format("csv")\
    .option("header", True)\
    .schema(ordersSchemaDDL)\
    .option("path", "C:/Users/chnis/Downloads/Big Data/week11/orders.csv")\
    .load

import spark.implicits._


orderDS = ordersDf.as[OrdersData]
ordersDf.printSchema
ordersDf.show

spark.stop()