from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("operations-dataframe").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

csv_file_path = "./data/products.csv"

df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

selected_columns = df.select("id", "name", "price")
print("Selected columns:")
selected_columns.show(10)

filtered_data = df.filter(df.quantity > 20)
print("Filtered data:", filtered_data.count())
filtered_data.show()

grouped_data = df.groupBy("category").agg({"quantity": "sum", "price": "avg"})
print("Grouped and Aggregated Data:")
grouped_data.show()

parquet_file_path = "./data/products.parquet"

df2 = spark.read.parquet(parquet_file_path).select("id","price")

joined_data = df.join(df2, "id", "inner")

print("Joined data:")

joined_data.show()


sorted_data = df.orderBy(col("category").desc(), col("price").desc())
print("Sorted Data:")
sorted_data.show()

df_with_new_column = df.withColumn("revenue", df.quantity * df.price)
print("DataFrame with new column:")
df_with_new_column.show()
