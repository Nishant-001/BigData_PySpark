from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
orderDf = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "C:/Users/chnis/Downloads/Big Data/week12 -Spark4(Structured Api 2)/orders.csv") \
    .load()
orderDf.createOrReplaceTempView("orders")
resultDf = spark.sql("select order_status, count(*) as total_orders from orders group by order_status")

resultDf.write.format("csv")\
    .mode("Overwrite")\
    .option("path","C:/Users/chnis/Downloads/Big Data/week12 -Spark4(Structured Api 2)/SparkSQLOutput/")\
    .save()
resultDf.show()
