from pyspark import SparkContext

sc = SparkContext('local', "wordcount")
input = sc.textFile("C:/Users/chnis/Downloads/Big Data/week 9/search_data.txt")
# one input row will give multiple output roms
words = input.flatMap(lambda x: x.split(" "))
# one input row will be giving one output row
word_counts = words.map(lambda x: (x, 1))
final_count = word_counts.reduceByKey(lambda x, y: x + y)
result = final_count.collect()
for a in result:
    print(a)