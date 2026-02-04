import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# --- configuration ---
load_dotenv()
hdfs_dir_gtfs_parquet = os.getenv('HDFS_DIR_GTFS_PARQUET')

spark = SparkSession.builder.appName('fetch-data-ztm-waw').getOrCreate()

# --- read file from HDFS ---
hdfs_path = f'{hdfs_dir_gtfs_parquet}/agency'
df = spark.read.parquet(hdfs_path, header=True, inferSchema=True)
print(f"Schemat pliku agency zapisanego w {hdfs_path}:")
df.printSchema()
df.show(5)

spark.stop()