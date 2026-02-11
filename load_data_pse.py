import os
from dotenv import load_dotenv
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from utils import save_as_parquet_to_hdfs

# --- configuration ---
load_dotenv()

hdfs_url = os.getenv('HDFS_URL')
hdfs_user = os.getenv('HDFS_USER')
hdfs_dir_pse_json= os.getenv('HDFS_DIR_PSE_JSON')
hdfs_dir_pse_parquet = os.getenv('HDFS_DIR_PSE_PARQUET')

# --- connect to HDFS ---
hdfs = InsecureClient(hdfs_url, user=hdfs_user)

# --- read txt files names from HDFS ---
file_list = hdfs.list(hdfs_dir_pse_json, status=False)


spark = SparkSession.builder.appName('load-data-pse').getOrCreate()

for file_name in file_list:
    save_as_parquet_to_hdfs(
        spark,
        hdfs_dir_pse_json,
        hdfs_dir_pse_parquet,
        file_name,
        explode_col='value'
    )

spark.stop()