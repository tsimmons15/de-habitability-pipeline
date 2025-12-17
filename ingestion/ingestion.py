#User-defined libraries
from lib.logger import setup_logger
from ingestion.ingestion_lib import usgs_import, weather_import, census_import
from datetime import datetime, timedelta
from pathvalidate import sanitize_filename

import sys
import os

#CSV Directory path
csv_dir = os.environ.get('csv_dir')
log_dir = os.environ.get('log_dir')


logger = setup_logger("main_ingestion", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s")
#API Calls, ingestion start
def start():
    if invalidDir(csv_dir) or invalidDir(log_dir):
        logger.error(f"One, or both, of the csv or log directories are invalid.\n'{csv_dir}', '{log_dir}'. Check the environment the pipeline is running in for these environment variables.")
        sys.exit(5)

    logger.info("Ingestion starting.")
    usgs_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    usgs_start = usgs_end - timedelta(days=1)
    logger.info(f"Ingestion called for period: {usgs_start} to {usgs_end}")

    logger.info("Starting the usgs pull.")
    sanitized_dt = sanitize_filename(usgs_end)
    usgs_filename = f"{sanitized_dt}_usgs_raw.csv"
    usgs_filename = usgs_filename.replace(' ', '_')
    logger.info(f"USGS raw data directory: {csv_dir}/{usgs_filename}")
    usgs_import("usgs_raw", f"{csv_dir}/{usgs_filename}", usgs_start, usgs_end)
    
    logger.info("Starting the census and geocode pull.")
    census_filename = f"{sanitized_dt}_census_raw.csv"
    census_filename = census_filename.replace(' ', '_')
    logger.info(f"Census raw data directory: {csv_dir}/{census_filename}")
    geocode_filename = f"{sanitized_dt}_geocode_raw.csv"
    geocode_filename = geocode_filename.replace(' ', '_')
    logger.info(f"Geocode raw data directory: {csv_dir}/{geocode_filename}")

    geocode_json = census_import("census_raw", f"{csv_dir}/{census_filename}", "geocode_raw", f"{csv_dir}/{geocode_filename}")

    logger.info("Starting the weather pull.")
    weather_filename = f"{sanitized_dt}_weather_raw.csv"
    weather_filename = weather_filename.replace(' ', '_')
    logger.info(f"Weather raw data directory: {csv_dir}/{weather_filename}")
    for g in geocode_json:
        dt = usgs_start
        while dt <= usgs_end:
            logger.info(f"Running weather import for {dt.strftime('%Y-%m-%d')}, {g['lat']}, {g['lon']}")
            weather_import("weather_raw", f"{csv_dir}/{weather_filename}", dt.strftime("%Y-%m-%d"), g["lat"], g["lon"])
            dt = dt + timedelta(hours=1)
