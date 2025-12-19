from lib.logger import setup_logger

import requests, json, psycopg2, csv, io, os, sys, pandas as pd, re
from datetime import datetime


# API Endpoints
api_endpoints = {
    "usgs": "https://earthquake.usgs.gov/fdsnws/event/1/query",
    "weather": "https://api.openweathermap.org/data/3.0/onecall/day_summary",
    "geocode": "http://api.openweathermap.org/geo/1.0/direct",
    "census":"https://api.census.gov/data/2019/pep/charagegroups"
}

logger = setup_logger("ingestion_library", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s", debug=True)

#Pull in the USGS data
def usgs_import(table_name, usgs_file, pull_start, pull_end):
    payload = {
        'format':'geojson',
        'starttime':pull_start,
        'endtime':pull_end,
        'minmagnitude':'2'
    }
    r = requests.get(api_endpoints["usgs"], params=payload)

    json_obj = json.loads(r.text)
    if "features" not in json_obj:
        logger.error("Issue with the usgs data, no features object.")
        sys.exit(5)
    json_obj = json_obj["features"]
    if len(json_obj) <= 1 or "properties" not in json_obj[0]:
        logger.error("Issue with the usgs data, malformed features list. Length: {len(json_obj)} or 'properties' not in features list.")
        sys.exit(5)

    json_properties = json_obj[0]["properties"]

    if "geometry" not in json_obj[0]:
        logger.error("Issue with the usgs data, no geometry in features.")
        sys.exit(5)
    json_geometry = json_obj[0]["geometry"]
    
    if "coordinates" not in json_geometry:
        logger.error("Issue with the usgs data, no coordinates in the geometry feature.")
        sys.exit(5)
    json_geometry = json_geometry["coordinates"]
    
    if len(json_geometry) < 2:
        logger.warn("Issue with the usgs data, latitude or longitude is missing.")
        json_properties["latitude"] = None
        json_properties["longitude"] = None
    else:
        json_properties["latitude"] = json_geometry[1]
        json_properties["longitude"] = json_geometry[0]

    if "place" not in json_properties or len(json_properties["place"]) < 2:
        logger.warn("Issue with the usgs data, place is missing or improperly formed.")
        json_properties["place"] = None
    else:
        json_properties["place"] = json_properties["place"].replace(",", "|")

    if "title" not in json_properties or len(json_properties["title"]) < 2:
        logger.warn("Issue with the usgs data, title is missing or improperly formed.")
        json_properties["title"] = None
    else:
        json_properties["title"] = json_properties["title"].replace(",", "|")

    del json_properties["url"]
    del json_properties["detail"]
    del json_properties["sources"]
    del json_properties["types"]
    del json_properties["ids"]
    
    if "time" not in json_properties:
        logger.warn("Issue with the usgs data, time is missing.")
    else:
        json_properties["time"] = datetime.fromtimestamp(json_properties["time"]/1000)
    if "updated" not in json_properties:
        logger.warn("Issue with the usgs data, updated is missing.")
    else:
        json_properties["updated"] = datetime.fromtimestamp(json_properties["updated"]/1000)
    
    df = pd.json_normalize(json_properties)

    df.to_csv(usgs_file, index=False, encoding='utf-8')
    logger.info("CSV file for usgs data written.")

    # Get rid of stale data
    #if os.path.exists(usgs_file):
    #    os.remove(usgs_file)


def weather_import(table_name, weather_file, search_time, search_lat, search_lon):
    payload = {
        "lat":search_lat,
        "lon":search_lon,
        "date":search_time,
        "appid":api_config["weather_key"]
    }
    r = requests.get(api_endpoints["weather"], params=payload)

    json_obj = json.loads(r.text)
    weather_result = json_obj.copy()
    resetValueOrDefault(weather_result, ["cloud_cover"], weather_result, ["cloud_cover", "afternoon"])
    logger.info(f"Value of weather_result['cloud_cover']: {weather_result['cloud_cover']}")

    resetValueOrDefault(weather_result, ["humidity"], weather_result, ["humidity", "afternoon"])
    logger.info(f"Value of weather_result['humidity']: {weather_result['humidity']}")

    resetValueOrDefault(weather_result, ["precipitation"], weather_result, ["precipitation", "total"])
    logger.info(f"Value of weather_result['precipitation']: {weather_result['precipitation']}")

    resetValueOrDefault(weather_result, ["pressure"], weather_result, ["pressure", "afternoon"])
    logger.info(f"Value of weather_result['pressure']: {weather_result['pressure']}")

    resetValueOrDefault(weather_result, ["wind"], weather_result, ["wind", "max", "speed"])
    logger.info(f"Value of weather_result['wind']: {weather_result['wind']}")
    
    if "temperature" in weather_result:
        del weather_result["temperature"]
        weather_result["temperature_min"] = json_obj["temperature"]["min"]
        weather_result["temperature_max"] = json_obj["temperature"]["max"]
        logger.info(f"Temperatures: {weather_result['temperature_min']}, {weather_result['temperature_max']}")
    else:
        logger.warn("Temperature is missing from the weather pull. Setting temperatures to None")
        weather_result["temperature_min"] = None
        weather_result["temperature_max"] = None

    df = pd.json_normalize(weather_result)

    df.to_csv(weather_file, index=False, encoding='utf-8')
    logger.info("Weather data written to CSV.")

    # Get rid of stale data
    #if os.path.exists(weather_file):
    #    os.remove(weather_file)

def census_import(census_table, census_file, geocode_table, geocode_file):
    census_cols = ["name", "pop", "hisp", "state", "county"]
    geocode_cols = ["name", "lat", "lon", "country", "state"]
    
    pattern = re.compile("(.+) County")

    payload = {
        "get":"NAME,POP",
        "HISP":"0",
        "for":"county:*",
        "in":"state:*"
    }
    r = requests.get(api_endpoints["census"], params=payload)

    geocode_result = []
    census_result = []
    json_obj = json.loads(r.text)
    for c in json_obj[1:]:
        census_result.append(getDict(census_cols,c))
        (area, state) = c[0].split(",")
        logger.info(f"Census data found, finding geocode information for: ({area}, {state})")
        match = pattern.match(area)
        if match is not None or pattern.groups > 1:
            area_name = match.group(1)
        else:
            area_name = area

        
        geocode_payload = {
            "q":f"{area_name}",
            "limit":"5",
            "appid":api_config["weather_key"]
        }

        r = requests.get(api_endpoints["geocode"], params=geocode_payload)
        if r.status_code == requests.codes.ok and len(r.text) > 2:
            geocode_json = json.loads(r.text)
            for g in geocode_json:
                if len(g) > 50:
                    break
                if g["country"] == "US":
                    if "local_names" in g:
                        del g["local_names"]
                    logger.info(f"Geocode result found, appending {g}")
                    geocode_result.append(g)
    
        else:
            logger.warn(f"Unable to get details for {area_name}({area}) in {state}")

    census_df = pd.json_normalize(census_result)
    census_df.to_csv(census_file, index=False, encoding='utf-8')
    logger.info("Census data written to CSV")

    geocode_df = pd.json_normalize(geocode_result)
    geocode_df.to_csv(geocode_file, index=False, encoding='utf-8')
    logger.info("Geocode data written to CSV")

    # Get rid of stale data
    #if os.path.exists(census_file):
    #    os.remove(census_file)
    #if os.path.exists(geocode_file):
    #    os.remove(geocode_file)

    return geocode_result

def getDict(keys, list):
    return dict(zip(keys, list))

def uploadCSV(table_name, csv_file, cols, sep=','):
    print(f"Uploading data from {csv_file} to {table_name} using {cols}")
    conn = None
    try:
        print(db_config)
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Open the CSV file and use copy_from
        with open(csv_file, 'r') as fh:
            # Skip the header row if present in the CSV
            fh.readline()
            #cursor.copy_from(f, table_name, columns=cols, sep=sep)
            cursor.copy_expert(f"""
                COPY tsimmons.{table_name} (
                    {cols}
                )
                FROM STDIN
                WITH (
                    FORMAT csv,
                    DELIMITER ',',
                    QUOTE '"',
                    ESCAPE '\\'
                )
            """, fh)

        # Commit the transaction
        conn.commit()

    except Exception as e:
        logger.error(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()


def resetValueOrDefault(new_dict, new_key_list, old_dict, old_key_list, default=None):
    newValue = retrieveNestedValue(old_dict, old_key_list, default)
    logger.info(f"Retrieved newValue: {newValue}")

    del old_dict[old_key_list[0]]

    return setNestedValue(new_dict, new_key_list, newValue)


def setNestedValue(dictionary, key_list, value):
    current = dictionary
    length = len(key_list) - 1
    for (i, k) in enumerate(key_list):
        if k not in current or not isinstance(current[k], dict):
            if i == length:
                current[k] = value
                break
            else:
                current[k] = {}
        current = current[k]

    return True

def retrieveNestedValue(dictionary, key_list, default=None):
    result = dictionary
    for k in key_list:
        if not isinstance(result, dict) or k not in result:
            return default
        else:
            result = result[k]
    return result


def parseConf():
    db_text = os.environ.get('db_conf')
    weather_key = os.environ.get('weather_key')

    logger.info(f"The db_config text read in: {db_text}")
    logger.info(f"The weather key text read in: {weather_key}")
    if not db_text or not weather_key:
        logger.error("Unable to parse weather api key or the db configuration environment variables. Please check them.")
        sys.exit(4)

    global db_config 
    db_config = json.loads(db_text)
    logger.info(f"The json parsed db_config: {db_config}")
    global api_config 
    api_config = {"weather_key":weather_key}
    logger.info(f"The parsed weather key: {api_config}")

def invalidDirectory(directory):
    return not directory or not  os.path.exists(directory)


# db configuration and api key details
db_config = {} 
api_config = {}
