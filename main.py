#User-defined libraries
from lib import usgs_import, weather_import, census_import

#API Calls
usgs_import("usgs_raw", "usgs_raw.csv")

weather_import("weather_raw", "weather_raw.csv")

census_import("census_raw", "census_raw.csv")



