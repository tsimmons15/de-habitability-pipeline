#User-defined libraries
from lib import usgs_import, weather_import, census_import
from datetime import datetime, timedelta

#CSV Directory path
csv_dir = r"/home/ec2-user/ukus18nov/tsimmons/project/raw_data"

#API Calls
usgs_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
usgs_start = usgs_end - timedelta(days=1)

usgs_import("usgs_raw", f"{csv_dir}/usgs_raw.csv", usgs_start, usgs_end)

geocode_json = census_import("census_raw", f"{csv_dir}/census_raw.csv", "geocode_raw", f"{csv_dir}/geocode_raw.csv")

print(geocode_json)
for g in geocode_json:
    dt = usgs_start
    while dt <= usgs_end:
        #print(f"Running weather import for {dt}, {g['lat']}, {g['lon']}")
        weather_import("weather_raw", f"{csv_dir}/weather_raw.csv", dt.timestamp(), g["lat"], g["lon"])
        dt = dt + timedelta(hours=1)
