from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, first, countDistinct, last, row_number, rank, dense_rank, lag, \
    lead, ntile, percent_rank, cume_dist
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame
df = spark.createDataFrame([(1, "John", 25), (2, "Jane", 30), (3, "Alice", 28)], ["id", "name", "age"])

# select
df_select = df.select("name", "age")

# filter or where
df_filtered = df.filter(col("age") > 25)

# groupBy and aggregation
df_grouped = df.groupBy("name").agg(sum("age").alias("total_age"), count("name").alias("count"))

# orderBy or sort
df_sorted = df.orderBy("age")

# withColumn or withColumnRenamed
df_with_column = df.withColumn("is_adult", col("age") >= 18)

# drop
df_dropped = df.drop("age")

# distinct
df_distinct = df.distinct()

# limit
df_limited = df.limit(2)

# join
df2 = spark.createDataFrame([(1, "Manager"), (2, "Developer")], ["id", "role"])
df_joined = df.join(df2, "id", "inner")

# ================================Actions=======================================================================

# show
df.show()

# count
row_count = df.count()

# collect
rows = df.collect()

# take
few_rows = df.take(2)

# first
first_row = df.first()

# head
few_rows1 = df.head(2)


# foreach
def print_row(row):
    print(row)


df.foreach(print_row)

# write
df.write.format("csv").save("output.csv")

# =======================================Aggegations =====================================

# sum, avg, mean
df.agg(sum("age").alias("total_age"), avg("age").alias("average_age"))

# min, max
df.agg(min("age").alias("min_age"), max("age").alias("max_age"))

# count, countDistinct
df.agg(count("*").alias("total_rows"), countDistinct("age").alias("distinct_ages"))

# first, last
df.agg(first("name").alias("first_name"), last("name").alias("last_name"))

# pivot
df.groupBy("name").pivot("age").count()

# crosstab
df.crosstab("name", "age")

# ==========================================Window Functions==========================

window_spec = Window.orderBy("age")

# row_number
df.withColumn("row_number", row_number().over(window_spec))

# rank
df.withColumn("rank", rank().over(window_spec))

# dense_rank
df.withColumn("dense_rank", dense_rank().over(window_spec))

# lag
df.withColumn("previous_age", lag("age", 1).over(window_spec))

# lead
df.withColumn("next_age", lead("age", 1).over(window_spec))

# ntile
df.withColumn("tile", ntile(3).over(window_spec))

# percent_rank
df.withColumn("percent_rank", percent_rank().over(window_spec))

# cume_dist
df.withColumn("cume_dist", cume_dist().over(window_spec))
