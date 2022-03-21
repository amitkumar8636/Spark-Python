from pyspark import SparkContext

spark = SparkContext("local[*]")

file = spark.textFile("../data/customerorders-201008-180523.csv")
data = file.map(lambda x: x.split(","))
relevent_data =  data.map(lambda x: (x[0],float(x[2])))
final_data = relevent_data.reduceByKey(lambda x,y: x+y)
sorted_data = final_data.sortBy(lambda x: x[1],ascending=False)
result = sorted_data.collect()
print(result)
