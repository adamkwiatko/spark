import os
from dotenv import load_dotenv
from hdfs import InsecureClient
from pyspark.sql import SparkSession

# --- configuration ---
load_dotenv()

hdfs_url = os.getenv('HDFS_URL')
hdfs_user = os.getenv('HDFS_USER')
hdfs_dir_gtfs_txt = os.getenv('HDFS_DIR_GTFS_TXT')
hdfs_dir_gtfs_parquet = os.getenv('HDFS_DIR_GTFS_PARQUET')


spark = SparkSession.builder.appName('fetch-data-ztm-waw').getOrCreate()

# --- connect to HDFS ---
hdfs = InsecureClient(hdfs_url, user=hdfs_user)

# --- read txt files names ---
file_list = hdfs.list(hdfs_dir_gtfs_txt, status=False)

# --- save each file as parquet ---
for file_name in file_list:
    with hdfs.read(f'{hdfs_dir_gtfs_txt}/{file_name}') as txt_file:
        hdfs_path = f'{hdfs_dir_gtfs_txt}/{file_name}'
        df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
        print(f"Schemat pliku {file_name} zapisanego w {hdfs_path}:")
        df.printSchema()
        df.show(5)
        df.write.parquet(f"{hdfs_dir_gtfs_parquet}/{file_name.split(".")[0]}", mode="overwrite")

spark.stop()