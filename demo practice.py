from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import struct, col
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, TimestampType

spark_conf = SparkConf().set("spark.app.name", "practice") \
    .set("spark.master", "local[*]")

spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .getOrCreate()
ordess_schema_ddlstring = "order_id int,order_date timestamp, order_customer_id int,order_status string"

orders_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("order_date", TimestampType()),
    StructField("order_customer_id", IntegerType()),
    StructField("order_status", StringType())
])

orders_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(ordess_schema_ddlstring) \
    .option("path", "C:/Users/chnis/Downloads/Big Data/week11 -Spark3(Structured Api 1)/orders.csv").load()


class Orders:
    def __init__(self, order_id, order_date, order_customer_id, orderstatus):
        self.order_id = order_id
        self.order_date = order_date
        self.order_customer_id = order_customer_id
        self.orderstatus = orderstatus


orders_df.createOrReplaceTempView("orders")

# orders_df.as([Orders])

# orders_ds = orders_df.rdd.toDs()


orders_df.printSchema()
orders_df.show(10)
