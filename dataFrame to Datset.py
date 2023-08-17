from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()


# Define a case class
class MyClass:
    def __init__(self, id, name):
        self.id = id
        self.name = name


# Create a DataFrame
df = spark.createDataFrame([(1, "John"), (2, "Jane"), (3, "Alice")], ["id", "name"])

# Convert DataFrame to Dataset using the case class
dataset = df.as[MyClass]

dataset = df.as[(int, str)]

# Convert DataFrame to Dataset with column names
dataset1 = df.as[(int, str)].alias("new_id", "new_name")

dataset = df.rdd.map(lambda row: MyClass(row.id, row.name)).toDF()

dataset2 = df.toDF("new_id", "new_name")

dataset2.printSchema()

# Show the Dataset
dataset2.show()
