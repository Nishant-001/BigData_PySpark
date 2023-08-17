from pyspark import SparkContext
# Set the log level to only print errors
sc = SparkContext("local[*]", "LogLevelCount")
sc.setLogLevel("WARN")
# Create a SparkContext using every core of the local machine
base_rdd = sc.textFile("C:/Users/chnis/Downloads/Big Data/week 10/bigLog.txt")
mapped_rdd = base_rdd.map(lambda x: (x.split(":")[0], 1))
reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: x+y)
result = reduced_rdd.collect()
for x in result:
    print(x)