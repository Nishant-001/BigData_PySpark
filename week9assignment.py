from pyspark import SparkContext

sc = SparkContext("local[*]", "week9Assignment")
sc.setLogLevel("WARN")


def check_age(line):
    fields = line.split(",")
    if int(fields[1]) > 18:
        return fields[0], fields[1], fields[2], "Y"
    else:
        return fields[0], fields[1], fields[2], "N"


base_rdd = sc.textFile("C:/Users/chnis/Downloads/Big Data/week9 -Spark1/age.txt")

mapped_rdd = base_rdd.map(lambda line: check_age(line))

result = mapped_rdd.collect()
for x in result:
    print(x)
