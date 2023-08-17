from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, first, countDistinct, last, row_number, rank, dense_rank, lag, \
    lead, ntile, percent_rank, cume_dist, when, expr
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame
df = spark.createDataFrame([(1, "John", 25), (2, "Jane", 30), (3, "Alice", 28)], ["id", "name", "age"])

df_with_column = df.withColumn("is_adult", when(col("age") >= 18, "adult").otherwise("Young"))

df_with_case = df.withColumn("is_adult", expr("CASE WHEN age >= 18 THEN 'Yes' ELSE 'No' END"))

df_with_column.show()
df_with_case.show()