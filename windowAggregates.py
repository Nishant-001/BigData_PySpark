from pyspark import SparkConf
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
invoiceDF = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "C:/Users/chnis/Downloads/Big Data/week12 -Spark4(Structured Api 2)/windowdata.csv") \
    .load()
# myWindow = Window.partitionBy("country") \
#     .orderBy("weeknum") \
#     .rowsBetween(-1, 2)
# mydf = invoiceDF.withColumn("RunningTotal", sum("invoicevalue").over(myWindow))

invoiceDF.createOrReplaceTempView("Invoice")

# spark.sql("select country,sum(invoicevalue) Over(partition by country order by weeknum rows between unbounded "
#           "Preceding and current row)"
#           " from Invoice group by country").show()

spark.sql("""
    SELECT country, weeknum, invoicevalue,
           SUM(invoicevalue) OVER (PARTITION BY country ORDER BY weeknum ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_invoicevalue
    FROM Invoice
    group by country
    ORDER BY country, weeknum
""").show()
# mydf.show()
