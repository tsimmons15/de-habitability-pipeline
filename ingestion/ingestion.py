#User-defined libraries
from ingestion_lib import usgs_import, weather_import, census_import
from datetime import datetime, timedelta

#CSV Directory path
csv_dir = r"/home/ec2-user/ukus18nov/tsimmons/project/raw_data"

#API Calls
usgs_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
usgs_start = usgs_end - timedelta(days=1)

usgs_import("usgs_raw", f"{csv_dir}/usgs_raw.csv", usgs_start, usgs_end)

#geocode_json = census_import("census_raw", f"{csv_dir}/census_raw.csv", "geocode_raw", f"{csv_dir}/geocode_raw.csv")
geocode_json = [
        {"name":"Coahoma", "lat": 34.3667731, "lon": -90.5231561, "country": "US", "state": "Mississippi"},
        {"name":"Coahoma", "lat": 32.2961767, "lon": -101.3034455, "country": "US", "state": "Texas"},
        {"name":"Jasper", "lat": 30.9201995, "lon": -93.9965759, "country": "US", "state": "Texas"}
]
print(geocode_json)
i = 0
for g in geocode_json:
    if i > 50:
        break

    dt = usgs_start
    while dt <= usgs_end:
        print(f"Running weather import for {dt.strftime('%Y-%m-%d')}, {g['lat']}, {g['lon']}")
        weather_import("weather_raw", f"{csv_dir}/weather_raw.csv", dt.strftime("%Y-%m-%d"), g["lat"], g["lon"])
        dt = dt + timedelta(hours=1)
    i += 1
