#User-defined libraries
from lib.logger import setup_logger
from ingestion.ingestion_lib import usgs_import, weather_import, census_import
from datetime import datetime, timedelta

#CSV Directory path
csv_dir = r"/home/ec2-user/ukus18nov/tsimmons/project/raw_data"
log_dir = r"/home/ec2-user/ukus18nov/tsimmons/project/log"


logger = setup_logger("main_ingestion", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s")
#API Calls, ingestion start
def start():
    logger.info("Ingestion starting.")
    usgs_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    usgs_start = usgs_end - timedelta(days=1)
    logger.info(f"Ingestion called for period: {usgs_start} to {usgs_end}")

    logger.info("Starting the usgs pull.")
    usgs_import("usgs_raw", f"{csv_dir}/usgs_raw.csv", usgs_start, usgs_end)

    logger.info("Starting the census and geocode pull.")
    geocode_json = census_import("census_raw", f"{csv_dir}/census_raw.csv", "geocode_raw", f"{csv_dir}/geocode_raw.csv")
    i = 0
    for g in geocode_json:
        if i > 50:
            break

        dt = usgs_start
        while dt <= usgs_end:
            logger.info(f"Running weather import for {dt.strftime('%Y-%m-%d')}, {g['lat']}, {g['lon']}")
            weather_import("weather_raw", f"{csv_dir}/weather_raw.csv", dt.strftime("%Y-%m-%d"), g["lat"], g["lon"])
            dt = dt + timedelta(hours=1)
        i += 1
