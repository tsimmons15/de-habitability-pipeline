#User-defined libraries
from lib.logger import setup_logger
from ingestion.ingestion_lib import usgs_import, weather_import, census_import, invalidDirectory, parseConf, uploadCSV, call_insert_truncate
from datetime import datetime, timedelta
from pathvalidate import sanitize_filename

import sys
import os

#CSV Directory path
csv_dir = os.environ.get('csv_dir')


logger = setup_logger("main_ingestion", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s", debug=True)
#API Calls, ingestion start
def start():
    if invalidDirectory(csv_dir):
        logger.error(f"One, or both, of the csv or log directories are invalid.\n'{csv_dir}'. Check the environment the pipeline is running in for these environment variables.")
        sys.exit(5)

    logger.info("Parsing configurations passed in...")
    parseConf()

    reprocess = os.environ.get('REPROCESS')
    logger.info(f"Reprocess called? {reprocess}")


    call_insert_truncate()

    logger.info("Ingestion starting.")
    usgs_end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    usgs_start = usgs_end - timedelta(days=1)
    logger.info(f"Ingestion called for period: {usgs_start} to {usgs_end}")

    logger.info("Starting the usgs pull.")
    sanitized_dt = sanitize_filename(usgs_end.strftime("%Y-%m-%d %H:%M:%S"))
    usgs_filename = f"{sanitized_dt}_usgs_raw.csv"
    usgs_filename = usgs_filename.replace(' ', '_')
    logger.info(f"USGS raw data directory: {csv_dir}/{usgs_filename}")
    
    if reprocess is not None and reprocess == 'False':
        usgs_import(f"{csv_dir}/{usgs_filename}", usgs_start, usgs_end)

    cols = "mag, place, time, updated, tz, felt, cdi, mmi, alert, status, tsunami, sig, net, code, nst, dmin, rms, gap, \"magType\", type, title, latitude, longitude"
    uploadCSV("usgs_insert", f"{csv_dir}/{usgs_filename}", cols=cols)
    logger.info("CSV file uploaded to postgresql.")
    
    logger.info("Starting the census and geocode pull.")
    census_filename = f"{sanitized_dt}_census_raw.csv"
    census_filename = census_filename.replace(' ', '_')
    logger.info(f"Census raw data directory: {csv_dir}/{census_filename}")
    geocode_filename = f"{sanitized_dt}_geocode_raw.csv"
    geocode_filename = geocode_filename.replace(' ', '_')
    logger.info(f"Geocode raw data directory: {csv_dir}/{geocode_filename}")

    geocode_json = {}
    if reprocess is not None and reprocess == 'False':
        geocode_json = census_import(f"{csv_dir}/{census_filename}", f"{csv_dir}/{geocode_filename}")
    else:
        data = []
        with open(f"{csv_dir}/{census_filename}", mode='r', encoding='utf-8') as fh:
            # DictReader maps each row to a dictionary with header keys
            csv = csv.DictReader(fh)
            for row in csv:
                data.append(row)
            geocode_json = json.dumps(data)


    cols = "name, pop, hisp, state, county"
    uploadCSV("census_insert", f"{csv_dir}/{census_filename}", cols=cols)
    logger.info("Census CSV uploaded to postgresql.")

    cols = "name, lat, lon, country, state"
    uploadCSV("geocode_insert", f"{csv_dir}/{geocode_filename}", cols=cols)
    logger.info("Geocode CSV uploaded to postgresql.")

    logger.info("Starting the weather pull.")
    weather_filename = f"{sanitized_dt}_weather_raw.csv"
    weather_filename = weather_filename.replace(' ', '_')
    logger.info(f"Weather raw data directory: {csv_dir}/{weather_filename}")
    for g in geocode_json:
        dt = usgs_start
        while dt <= usgs_end:
            logger.info(f"Running weather import for {dt.strftime('%Y-%m-%d')}, {g['lat']}, {g['lon']}")
            if reprocess is not None and reprocess == 'False':
                weather_import(f"{csv_dir}/{weather_filename}", dt.strftime("%Y-%m-%d"), g["lat"], g["lon"])

            cols = "lat, lon, tz, date, units, cloud_cover, humidity, precipitation, temperature_min, temperature_max, pressure, wind"
            uploadCSV("weather_insert", f"{csv_dir}/{weather_filename}", cols=cols)
            dt = dt + timedelta(hours=1)


