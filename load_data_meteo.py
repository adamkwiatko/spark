import os
from dotenv import load_dotenv
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from utils import save_as_parquet_to_hdfs

# --- configuration ---
load_dotenv()

hdfs_url = os.getenv('HDFS_URL')
hdfs_user = os.getenv('HDFS_USER')
hdfs_dir_open_meteo_json= os.getenv('HDFS_DIR_OPEN_METEO_JSON')
hdfs_dir_open_meteo_parquet = os.getenv('HDFS_DIR_OPEN_METEO_PARQUET')

# --- connect to HDFS ---
hdfs = InsecureClient(hdfs_url, user=hdfs_user)

# --- read txt files names from HDFS ---
file_list = hdfs.list(hdfs_dir_open_meteo_json, status=False)

spark = SparkSession.builder.appName('load-data-meteo').getOrCreate()

for file_name in file_list:
    save_as_parquet_to_hdfs(
        spark,
        hdfs_dir_open_meteo_json,
        hdfs_dir_open_meteo_parquet,
        file_name,
        arrays_zip_col='hourly'
    )

spark.stop()