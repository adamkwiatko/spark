from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("pyspark-count-words").getOrCreate()

#rdd = spark.sparkContext.textFile("./text.txt")
#result_rdd = rdd.flatMap(lambda line: line.split(" ")) \
#        .map(lambda word: (word, 1)) \
#        .reduceByKey(lambda a, b: a + b) \
#        .sortBy(lambda x: x[1], ascending=False)

#print(result_rdd.take(100))

df = spark.read.text("./data/text.txt")

result_df = df.selectExpr("explode(split(value, ' ')) as word") \
        .groupBy("word").count().orderBy(desc("count"))

print(result_df.take(100))

spark.stop
