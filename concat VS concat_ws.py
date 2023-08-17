from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, concat

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame
df = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Smith"), (3, "Alice", None)], ["id", "first_name", "last_name"])

# Concatenate the "first_name" column with a non-column value
delimiter = " "
df1 = df.withColumn("full_name_ws", concat_ws(delimiter, col("first_name"), lit("Doe"))) # can handle null values
df2 = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))  # cannot handle null values
# Show the DataFrame
df1.show()
df2.show()