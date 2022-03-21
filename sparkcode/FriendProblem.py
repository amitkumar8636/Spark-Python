from pyspark import SparkContext

spark = SparkContext("local[*]", "FriendsProblem")
file = spark.textFile("../data/friendsdata-201008-180523.csv")

data = file.map(lambda x: x.split("::")).map(lambda x: (x[2], (float(x[3]),1))) \
    .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])) \
    .map(lambda x: (x[0],x[1][0]/x[1][1]))
result = data.collect()
print(result)
