from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("test-window").getOrCreate()

df = spark.read.csv("./data/superstore.csv", header=True, inferSchema=True, escape='"', quote='"')
df.printSchema()
df.show(10)
#df.createOrReplaceTempView("sales")
#dfs = spark.sql("SELECT country, city, count(*) as cnt from sales group by country, city")
#dfs.show()
#dfs.write.csv("./data/sales_result_by_city.csv")
window_spec = Window.partitionBy("Category").orderBy("`Order.Date`")
df = df.select("Category","City","`Order.Date`","Sales")
df = df.withColumn("running_total",sum("Sales").over(window_spec))
df.show()
