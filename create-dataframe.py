from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("create-dataframe").getOrCreate()

csv_file_path = "./data/products.csv"

schema = StructType([
    StructField(name="id", dataType=IntegerType(), nullable=True),
    StructField(name="name", dataType=StringType(), nullable=True),
    StructField(name="category", dataType=StringType(), nullable=True),
    StructField(name="quantity", dataType=IntegerType(), nullable=True),
    StructField(name="price", dataType=DoubleType(), nullable=True)
])

df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

#df.printSchema()
#df.show(5)

json_file_path = "./data/products_singleline.json"

df2 = spark.read.json(json_file_path)

#df2.printSchema()
#df2.show(5)

json_file_path_2 = "./data/products_multiline.json"

df3 = spark.read.json(json_file_path_2, multiLine=True)

#df3.printSchema()
#df3.show(5)

parquet_file_path = "./data/products.parquet"

df3.write.parquet(parquet_file_path, mode="overwrite")


df4 = spark.read.parquet(parquet_file_path)

df4.printSchema()
df4.show()

spark.stop()
