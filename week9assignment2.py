from pyspark import SparkContext

sc = SparkContext("local[*]", "week9assignment2")
sc.setLogLevel("WARN")

base_rdd = sc.textFile("C:/Users/chnis/Downloads/Big Data/week9 -Spark1/tempdata.csv")

mapped_rdd = base_rdd.map(lambda x: (x.split(",")[0], float(x.split(",")[3])))

filtered_rdd = mapped_rdd.filter(lambda x: x[1] == "TMIN")
final_rdd = mapped_rdd.reduceByKey(lambda x,y : min(x, y))

result = final_rdd.collect()

for x in result:
    print(x)
