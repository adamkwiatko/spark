import os
from dotenv import load_dotenv
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from utils import read_parquet_from_hdfs, create_timestamp_sin_cos, merge_delta_table

# --- configuration ---
load_dotenv()

hdfs_url = os.getenv('HDFS_URL')
hdfs_user = os.getenv('HDFS_USER')
hdfs_dir_pse_parquet = os.getenv('HDFS_DIR_PSE_PARQUET')
delta_lake_pse = os.getenv('DELTA_LAKE_PSE')

# --- connect to HDFS ---
hdfs = InsecureClient(hdfs_url, user=hdfs_user)

# --- read parquet files names from HDFS to upload ---
# file_list = hdfs.list(hdfs_dir_pse_parquet, status=False)
file_list = ['pk5l-wp_2025-12-01']

# --- create spark session with delta configuration ---
spark = (SparkSession.builder
         .appName('load-data-pse')
         .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
         .getOrCreate())


for file_name in file_list:

    df = read_parquet_from_hdfs(
        spark,
        hdfs_dir_pse_parquet,
        file_name
    )

    df.printSchema()
    df.show(5)

    # --- table transformation: change data type and add columns with calendar and time periods and sin / cos functions ---
    # df = create_timestamp_sin_cos(
    #     df,
    #     '???',
    #     to_timestamp_flg=True,
    #     create_periods=['year', 'month', 'dayofyear', 'hour']
    # )

    # merge_delta_table(spark, delta_lake_pse, df, 'time')

    # hdfs.delete(hdfs_dir_pse_parquet, recursive=True)

spark.stop()