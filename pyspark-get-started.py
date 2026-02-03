from pyspark.sql import SparkSession

spark = SparkSession.builder \
	.appName("pyspark-get-started") \
	.getOrCreate()


data = [("Alice", 25), ("Robert", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()
