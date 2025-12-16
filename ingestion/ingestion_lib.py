import requests, json, psycopg2, csv, io, os, sys, pandas as pd, re
from datetime import datetime


# API Endpoints
api_endpoints = {
    "usgs": "https://earthquake.usgs.gov/fdsnws/event/1/query",
    "weather": "https://api.openweathermap.org/data/3.0/onecall/day_summary",
    "geocode": "http://api.openweathermap.org/geo/1.0/direct",
    "census":"https://api.census.gov/data/2019/pep/charagegroups"
}


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
    json_properties = json_obj["features"][0]["properties"]
    json_geometry = json_obj["features"][0]["geometry"]["coordinates"]
    json_properties["latitude"] = json_geometry[1]
    json_properties["longitude"] = json_geometry[0]
    json_properties["place"] = json_properties["place"].replace(",", "|")
    json_properties["title"] = json_properties["title"].replace(",", "|")
    del json_properties["url"]
    del json_properties["detail"]
    del json_properties["sources"]
    del json_properties["types"]
    del json_properties["ids"]

    json_properties["time"] = datetime.fromtimestamp(json_properties["time"]/1000)
    json_properties["updated"] = datetime.fromtimestamp(json_properties["updated"]/1000)
    #print(json_properties)
    df = pd.json_normalize(json_properties)

    df.to_csv(usgs_file, index=False, encoding='utf-8')

    cols = "mag, place, time, updated, tz, felt, cdi, mmi, alert, status, tsunami, sig, net, code, nst, dmin, rms, gap, \"magType\", type, title, latitude, longitude"

    uploadCSV(table_name, usgs_file, cols=cols)

    # Get rid of stale data
    #if os.path.exists(usgs_file):
    #    os.remove(usgs_file)


def weather_import(table_name, weather_file, search_time, search_lat, search_lon):
    payload = {
        "lat":search_lat,
        "lon":search_lon,
        "dt":search_time,
        "appid":api_config["weather_key"]
    }
    r = requests.get(api_endpoints["weather"], params=payload)
    #print(r.text)

    json_obj = json.loads(r.text)
    weather_result = json_obj.copy()
    if "cloud_cover" in weather_result:
        del weather_result["cloud_cover"]
        weather_result["cloud_cover"] = json_obj["cloud_cover"]["afternoon"]
    else:
        weather_result["cloud_cover"] = None
    if "humidity" in weather_result:
        del weather_result["humidity"]
        weather_result["humidity"] = json_obj["humidity"]["afternoon"]
    else:
        weather_result["humidity"] = None
    if "precipitation" in weather_result:
        del weather_result["precipitation"]
        weather_result["precipitation"] = json_obj["precipitation"]["total"]
    else:
        weather_result["precipitation"] = None
    if "temperature" in weather_result:
        del weather_result["temperature"]
        weather_result["temperature_min"] = json_obj["temperature"]["min"]
        weather_result["temperature_max"] = json_obj["temperature"]["max"]
    else:
        weather_result["temperature_min"] = None
        weather_result["temperature_max"] = None
    if "pressure" in weather_result:
        del weather_result["pressure"]
        weather_result["pressure"] = json_obj["pressure"]["afternoon"]
    else:
        weather_result["pressure"] = None
    if "wind" in weather_result:
        del weather_result["wind"]
        weather_result["wind"] = json_obj["wind"]["max"]["speed"]
    else:
        weather_result["wind"] = None

    df = pd.json_normalize(weather_result)

    df.to_csv(weather_file, index=False, encoding='utf-8')

    cols = "lat, lon, tz, date, units, cloud_cover, humidity, precipitation, temperature_min, temperature_max, pressure, wind"

    uploadCSV(table_name, weather_file, cols)

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
                    geocode_result.append(g)
    
        else:
            #print(f"Unable to get details for {area_name}({area}) in {state}")
            pass


    census_df = pd.json_normalize(census_result)
    census_df.to_csv(census_file, index=False, encoding='utf-8')

    geocode_df = pd.json_normalize(geocode_result)
    geocode_df.to_csv(geocode_file, index=False, encoding='utf-8')

    uploadCSV(census_table, census_file, ", ".join(census_cols))
    uploadCSV(geocode_table, geocode_file, ", ".join(geocode_cols))

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
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()


def readConf(conf_file):
    result = ""
    with open(conf_file, "r") as fh:
        result = json.loads(fh.read())
    return result

conf_root = r"/home/ec2-user/ukus18nov/tsimmons/conf/"
conf_files = {
    "db":"db.conf", "api":"api.conf"
}

# db configuration details
db_config = readConf(conf_root + conf_files["db"])
api_config = readConf(conf_root + conf_files["api"])

