from hdfs import InsecureClient
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, explode, arrays_zip, col, to_timestamp, \
    year, month, dayofyear, hour, lit, sin, cos
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from io import BytesIO
from zipfile import ZipFile
from delta.tables import DeltaTable
import requests
import math


# --- read file from HDFS ---
def read_parquet_from_hdfs(spark_session: SparkSession, dir: str, file_name: str) -> DataFrame:
    hdfs_path = f'{dir}/{file_name}'
    df = spark_session.read.parquet(hdfs_path, header=True, inferSchema=True)
    return df


# --- save files to HDFS ---
def save_as_parquet_to_hdfs(
        spark_session: SparkSession,
        hdfs_path: str,
        parquet_dir: str,
        file_name: str,
        explode_col: str = None,
        arrays_zip_col: str = None
):

    hdfs_path = f'{hdfs_path}/{file_name}'
    file_type = file_name.split(".")[-1]
    if file_type in ['csv', 'txt']:
        df = spark_session.read.csv(hdfs_path, header=True, inferSchema=True)
    elif file_type == 'json':
        df = spark_session.read.json(hdfs_path, multiLine=True)
        if explode_col:
            df = df.select(explode(f"{explode_col}").alias(f"{explode_col}")).select(f"{explode_col}.*")
        if arrays_zip_col:
            array_fields = df.select(f"{arrays_zip_col}.*").schema.fieldNames()
            zipped = arrays_zip(*[col(f"{arrays_zip_col}.{c}") for c in array_fields])
            df = df.select(*[col(c) for c in df.columns if c != "f{arrays_zip_col}"], explode(zipped).alias("z"))
            df = df.select(*[col(c) for c in df.columns if c != "z"], *[col(f"z.{c}").alias(c) for c in array_fields])
            df = df.drop(f"{arrays_zip_col}")
    df = df.withColumn("insert_date", current_timestamp())
    df.write.parquet(f'{parquet_dir}/{file_name.split(".")[0]}', mode="overwrite")



def fetch_zip_to_hdfs(source_url: str, hdfs_url: str, hdfs_user: str, hdfs_dir: str):

    # --- connect to HDFS ---
    hdfs = InsecureClient(hdfs_url, user=hdfs_user)
    hdfs.makedirs(hdfs_dir)

    zip_buffer = BytesIO()

    # download latest archive file
    with requests.get(source_url, stream=True) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=8192):
            zip_buffer.write(chunk)

    # unzip archive file
    with ZipFile(zip_buffer, 'r') as zip_ref:
        for name in zip_ref.namelist():
            with zip_ref.open(name) as file_in_zip:
                hdfs_path = f"{hdfs_dir}/{name}"
                with hdfs.write(hdfs_path, overwrite=True) as hdfs_file:
                    hdfs_file.write(file_in_zip.read())


def fetch_json_to_hdfs(
        source_url: str,
        hdfs_url: str,
        hdfs_user: str,
        hdfs_dir: str,
        json_name: str,
        source_params: dict,
        timeout: int = 20
):

    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    hdfs = InsecureClient(hdfs_url, user=hdfs_user)
    hdfs.makedirs(hdfs_dir)

    try:
        response = session.get(source_url, params=source_params, timeout=timeout)
        response.raise_for_status()
        hdfs_path = f'{hdfs_dir}/{json_name}'
        with hdfs.write(hdfs_path, overwrite=True) as hdfs_file:
            hdfs_file.write(response.content)
    except requests.exceptions.RequestException as e:
        print(e)


def create_timestamp_sin_cos(
        df: DataFrame,
        timestamp_col: str,
        to_timestamp_flg: bool = False,
        create_periods: list[tuple] = None
) -> DataFrame:

    if to_timestamp_flg:
        df = df.withColumn(timestamp_col, to_timestamp(timestamp_col))

    if create_periods:
        for period_type, period_max in create_periods:
            func = globals()[period_type]
            df = df.withColumn(f'{timestamp_col}_{period_type}', func(timestamp_col))
            if period_max:
                df = df.withColumn(f'{timestamp_col}_{period_type}_sin',
                                   sin(lit(2) * lit(math.pi) * col(f'{timestamp_col}_{period_type}') / lit(period_max)))
                df = df.withColumn(f'{timestamp_col}_{period_type}_cos',
                                   cos(lit(2) * lit(math.pi) * col(f'{timestamp_col}_{period_type}') / lit(period_max)))
    return df

def merge_delta_table(
        spark: SparkSession,
        delta_table_location: str,
        df: DataFrame,
        key_col: str
):
    DeltaTable.createIfNotExists(spark) \
        .location(delta_table_location) \
        .addColumns(df.schema) \
        .execute()

    DeltaTable.forPath(spark, delta_table_location) \
        .alias('existing') \
        .merge(
            df.alias('new'),
            f'existing.{key_col} = new.{key_col}'
    ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()