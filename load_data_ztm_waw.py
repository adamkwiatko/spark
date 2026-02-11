import os
from dotenv import load_dotenv
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from utils import save_as_parquet_to_hdfs

# --- configuration ---
load_dotenv()

hdfs_url = os.getenv('HDFS_URL')
hdfs_user = os.getenv('HDFS_USER')
hdfs_dir_gtfs_txt = os.getenv('HDFS_DIR_GTFS_TXT')
hdfs_dir_gtfs_parquet = os.getenv('HDFS_DIR_GTFS_PARQUET')

# --- connect to HDFS ---
hdfs = InsecureClient(hdfs_url, user=hdfs_user)

# --- read txt files names from HDFS ---
file_list = hdfs.list(hdfs_dir_gtfs_txt, status=False)

# --- save files to parquet ---
spark = SparkSession.builder.appName('load-data-ztm-waw').getOrCreate()
for file_name in file_list:
    hdfs_dir_with_name = f'{hdfs_dir_gtfs_parquet}/{file_name.split(".")[0]}'
    save_as_parquet_to_hdfs(spark, hdfs_dir_gtfs_txt, hdfs_dir_with_name, file_name)
spark.stop()