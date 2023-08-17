from pyspark import SparkContext

sc= SparkContext("local[*]", "ratings")
ratings_rdd = sc.textFile("C:/Users/chnis/Downloads/Big Data/week11/ratings.dat")

movie_rdd = sc.textFile("C:/Users/chnis/Downloads/Big Data/week11/movies.dat")

mapped_ratings_rdd = ratings_rdd.map(lambda x:  (x.split("::")[1],x.split("::")[2]))

mapped_movies_rdd = movie_rdd.map(lambda x:  (x.split("::")[0],(x.split("::")[1],x.split("::")[2])))

new_mapped = mapped_ratings_rdd.mapValues(lambda x: (float(x), 1.0))

reducedRdd = new_mapped.reduceByKey(lambda x, y : (x[0] + y[0], x[1] + y[1]))

filteredRDD = reducedRdd.filter(lambda x : x[1][1] > 1000)

topRating = filteredRDD.mapValues(lambda x : x[0] / x[1]).filter(lambda x : x[1] > 4.0)

result = topRating.collect()


joinedRDD = mapped_movies_rdd.join(topRating)

finalres = joinedRDD.map(lambda x: x[1][0])

res = finalres.collect()

for x in res:
    print(x)