from dotenv import load_dotenv
from utils import fetch_json_to_hdfs
from datetime import timedelta, date
import os

load_dotenv()

api_url = os.getenv('API_PSE')
hdfs_url = os.getenv('HDFS_URL')
hdfs_user = os.getenv('HDFS_USER')
hdfs_dir_pse_json= os.getenv('HDFS_DIR_PSE_JSON')

start_date = date(2025, 12, 4)
end_date = date(2025,12, 31)

current_date = start_date

while current_date <= end_date:
    json_name = f'pk5l-wp_{current_date.isoformat()}.json'
    source_params = {'$filter': f"business_date eq '{current_date.isoformat()}'"}
    fetch_json_to_hdfs(api_url, hdfs_url, hdfs_user, hdfs_dir_pse_json, json_name, source_params)
    current_date += timedelta(days=1)