from dotenv import load_dotenv
from utils import fetch_zip_to_hdfs
import os

load_dotenv()

api_url = os.getenv('API_ZTM_WAW')
hdfs_url = os.getenv('HDFS_URL')
hdfs_user = os.getenv('HDFS_USER')
hdfs_dir_gtfs_txt = os.getenv('HDFS_DIR_GTFS_TXT')

fetch_zip_to_hdfs(api_url, hdfs_url, hdfs_user, hdfs_dir_gtfs_txt)
