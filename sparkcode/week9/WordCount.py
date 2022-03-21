from logging import Logger

from pyspark import SparkContext


spark = SparkContext("local[*]", "TotalSpentByCustomerSorted")
spark.setLogLevel("ERROR")
file = spark.textFile("../data/search_data-201008-180523.txt")

words = file.flatMap(lambda x: x.split(" "))
wordsMap = words.map(lambda x: (x,1))
print(wordsMap.collect())


