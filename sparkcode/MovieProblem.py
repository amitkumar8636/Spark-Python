from pyspark import SparkContext

spark = SparkContext("local[*]","MovieProblem")
file = spark.textFile("../data/moviedata-201008-180523.data")

data = file.map(lambda x: (x.split("\t")[2],1)).reduceByKey(lambda x,y: x+y)
result = data.collect()

print(result)
