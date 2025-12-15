import requests, json, psycopg2, csv, io, os, sys, pandas as pd, re
from datetime import datetime


# API Endpoints
api_endpoints = {
    "usgs": "https://earthquake.usgs.gov/fdsnws/event/1/query",
    "weather": "https://api.openweathermap.org/data/3.0/onecall/timemachine",
    "geocode": "http://api.openweathermap.org/geo/1.0/direct",
    "census":"https://api.census.gov/data/2019/pep/charagegroups"
}


#Pull in the USGS data
def usgs_import(table_name, csv_file, pull_start, pull_end):
    payload = {
        'format':'geojson',
        'starttime':pull_start,
        'endtime':pull_end,
        'minmagnitude':'5'
    }
    r = requests.get(api_endpoints["usgs"], params=payload)

    json_obj = json.loads(r.text)
    json_properties = json_obj["features"][0]["properties"]
    json_geometry = json_obj["features"][0]["geometry"]["coordinates"]
    json_properties["latitude"] = json_geometry[1]
    json_properties["longitude"] = json_geometry[0]
    del json_properties["url"]
    del json_properties["detail"]
    json_properties["time"] = datetime.fromtimestamp(json_properties["time"]/1000)
    json_properties["updated"] = datetime.fromtimestamp(json_properties["updated"]/1000)
    df = pd.json_normalize(json_properties)

    df.to_csv(csv_file, index=False, encoding='utf-8')

    uploadCSV(table_name, csv_file)

    # Get rid of stale data
    if os.path.exists(csv_file):
        os.remove(csv_file)


def weather_import(table_name, csv_file, search_time, search_lat, search_lon):
    payload = {
        "lat":"38.83",
        "lon":"-122.83",
        "dt":"1765454400",
        "appid":api_config["weather_key"]
    }
    r = requests.get(api_endpoints["weather"], params=payload)
    #print(r.text)

    json_obj = json.loads(r.text)
    df = pd.json_normalize(json_obj)

    df.to_csv(csv_file, index=False, encoding='utf-8')

    uploadCSV(table_name, csv_file)

    # Get rid of stale data
    if os.path.exists(csv_file):
        os.remove(csv_file)

def census_import(table_name, csv_file, geocode_table, geocode_file):
    pattern = re.compile("(.+) County")

    payload = {
        "get":"NAME,POP",
        "HISP":"0",
        "for":"county:*",
        "in":"state:*"
    }
    r = requests.get(api_endpoints["census"], params=payload)

    geocode_result = []
    json_obj = json.loads(r.text)
    for c in json_obj[1:]:
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
                if g["country"] == "US":
                    geocode_result.append(g)

        else:
            print(f"Unable to get details for {area_name}({area}) in {state}")


    df = pd.json_normalize(json_obj)
    df.to_csv(csv_file, index=False, encoding='utf-8')

    geocode_df = pd.json_normalize(geocode_result)
    df.to_csv(geocode_file, index=False, encoding='utf-8')

    uploadCSV(table_name, csv_file)
    uploadCSV(geocode_table, geocode_file)

    # Get rid of stale data
    if os.path.exists(csv_file):
        os.remove(csv_file)
    if os.path.exists(geocode_file):
        os.remove(geocode_file)

    return geocode_result

def uploadCSV(table_name, csv_file, sep=','):
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Open the CSV file and use copy_from
        with open(csv_file, 'r') as f:
            # Skip the header row if present in the CSV
            f.readline()
            cursor.copy_from(f, table_name, sep=sep)

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

