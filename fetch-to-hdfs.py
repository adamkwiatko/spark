import requests
import os
from dotenv import load_dotenv
from io import BytesIO
from zipfile import ZipFile
from hdfs import InsecureClient

# --- configuration ---
load_dotenv()

api_ztm_waw = os.getenv('API_ZTM_WAW')
hdfs_url = os.getenv('HDFS_URL')
hdfs_dir_gtfs_txt = os.getenv('HDFS_DIR_GTFS_TXT')
hdfs_dir_gtfs_parquet = os.getenv('HDFS_DIR_GTFS_PARQUET')
hdfs_user = os.getenv('HDFS_USER')

# --- connect to HDFS ---
hdfs = InsecureClient(hdfs_url, user=hdfs_user)
hdfs.makedirs(hdfs_dir_gtfs_txt)

zip_buffer = BytesIO()

# download latest archive file
with requests.get(api_ztm_waw, stream=True) as r:
    r.raise_for_status()
    for chunk in r.iter_content(chunk_size=8192):
        zip_buffer.write(chunk)

# unzip archive file
with ZipFile(zip_buffer, 'r') as zip_ref:
    for name in zip_ref.namelist():
        with zip_ref.open(name) as file_in_zip:
            hdfs_path = f"{hdfs_dir_gtfs_txt}/{name}"
            with hdfs.write(hdfs_path, overwrite=True) as hdfs_file:
                hdfs_file.write(file_in_zip.read())