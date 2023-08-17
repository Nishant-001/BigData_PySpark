from pyspark import SparkContext
def blankLineChecker(line):
    if(len(line) == 0):
        myaccum.add(1)

sc = SparkContext("local[*]","AccumulatorExample")
myrdd = sc.textFile("C:/Users/chnis/Downloads/Big Data/week 10/samplefile.txt")
myaccum = sc.accumulator(0.0)
myrdd.foreach(blankLineChecker)
print(myaccum.value)