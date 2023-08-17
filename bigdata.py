from pyspark import SparkContext

def load_boring_words():
    boring_words= set(line.strip()for line in open("C:/Users/chnis/Downloads/Big Data/week 10/boringwords.txt"))
    return boring_words

sc = SparkContext("local[*]", "KeywordAmount")
name_set = sc.broadcast(load_boring_words())
initial_rdd = sc.textFile("C:/Users/chnis/Downloads/Big Data/week 10/bigdatacampaigndata.csv")
mapped_input = initial_rdd.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))
words = mapped_input.flatMapValues(lambda x: x.split(" "))
final_mapped = words.map(lambda x: (x[1].lower(), x[0]))
mapped2 = final_mapped.filter(lambda x: x[0] not in name_set.value)
total = mapped2.reduceByKey(lambda x, y: x+y)
sorted1 = total.sortBy(lambda x: x[1],False)
result = sorted1.take(20)
for i in result:
    print(i)