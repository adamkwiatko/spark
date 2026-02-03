from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
#from ftplib import FTP
import requests
import zipfile
import os

# connect to ftp with arch
#ftp = FTP('gtfs.ztm.waw.pl')
#ftp.login()

#ftp.retrlines('LIST')

#ftp.quit()

url_last = 'https://gtfs.ztm.waw.pl/last'
arch_last = "./data/last.zip"
dir_last = "./data/last"

with requests.get(url_last, stream=True) as r:
    r.raise_for_status()
    with open(arch_last, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

with zipfile.ZipFile(arch_last, 'r') as zip_ref:
    zip_ref.extractall(dir_last)

spark = SparkSession.builder.appName('fetch-data-ztm-waw').getOrCreate()

for csv_file in os.listdir(dir_last):
    csv_file_path = dir_last + "/" + csv_file
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    df = df.withColumn("insert_date", current_timestamp())
    print("Schemat pliku ", csv_file)
    df.printSchema()
    df.show(5)
    parquet_file = dir_last + "/" + csv_file.split(".")[0] + ".parquet"
    df.write.parquet(parquet_file, mode="overwrite")

os.remove(arch_last)

spark.stop()
