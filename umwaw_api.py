from dotnew import load_dotnew
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

load_dotenv()

spark = SparkSession.builder.appName("pyspark-count-words").getOrCreate()

umwaw_api_key = os.getenv("UMWAW_API_KEY")

#przykład wywołania dla zapytania o wszystkie dostępne autobusy
# curl -X POST -H "Cache-Control: no-cache"
#https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id= f2e5503e-
#927d-4ad3-9500-4ab9e55deb59&apikey=tu_podaj_swoj_apikey&type=1
#przykład wywołania dla zapytania o wszystkie dostępne tramwaje
# curl -X POST -H "Cache-Control: no-cache"
#https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id= f2e5503e-
#927d-4ad3-9500-4ab9e55deb59&apikey=tu_podaj_swoj_apikey&type=2

#Dostępne są następujące informacje:
# Lat – współrzędna szerokości geograficznej w układzie WGS84(EPSG:4326)
# Lon – współrzędna długości geograficznej w układzie WGS84(EPSG:4326)
# Time – czas wysłania sygnału GPS
# Lines- numer linii autobusowej lub tramwajowej
# Brigade – numer brygady pojazdu
# VehicleNumber – numer taborowy pojazdu
