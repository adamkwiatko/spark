from dotenv import load_dotenv
from utils import fetch_json_to_hdfs
from datetime import date
import os

load_dotenv()

api_url = os.getenv('API_OPEN_METEO_HIST')
hdfs_url = os.getenv('HDFS_URL')
hdfs_user = os.getenv('HDFS_USER')
hdfs_dir_open_meteo_json = os.getenv('HDFS_DIR_OPEN_METEO_JSON')

start_date = date(2025, 12, 4)
end_date = date(2025,12, 31)
latitude = 52.13
longitude = 21.01
select_columns = ["temperature_2m", "cloud_cover", "wind_speed_10m", "wind_gusts_10m",
                  "relative_humidity_2m", "shortwave_radiation", "direct_radiation", "diffuse_radiation",
                  "global_tilted_irradiance", "direct_normal_irradiance", "terrestrial_radiation"
                  ]

json_name = f'arch_{start_date.isoformat()}_{end_date.isoformat()}_{str(round(latitude))}_{str(round(longitude))}.json'
source_params = {
    "timezone": "Europe/Berlin",
    "latitude": latitude,
    "longitude": longitude,
    "hourly": select_columns,
    "start_date": start_date.isoformat(),
    "end_date": end_date.isoformat(),
}

fetch_json_to_hdfs(api_url, hdfs_url, hdfs_user, hdfs_dir_open_meteo_json, json_name, source_params)
