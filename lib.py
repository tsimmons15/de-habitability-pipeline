import requests, json, psycopg2, csv, io, os, sys, pandas as pd

# API Endpoints
api_endpoints = {
    "usgs": "https://earthquake.usgs.gov/fdsnws/event/1/query",
    "weather": "https://api.openweathermap.org/data/3.0/onecall/timemachine",
    "census":"https://api.census.gov/data/2019/pep/charagegroups"
}


#Pull in the USGS data
def usgs_import(table_name, csv_file):
    payload = {
        'format':'geojson',
        'starttime':'2025-12-11T00:00:00.0000',
        'endtime':'2025-12-11T18:00:00.0000',
        'minmagnitude':'5'
    }
    r = requests.get(api_endpoints["usgs"], params=payload)
    print(r.text)

    json_obj = json.loads(r.text)
    df = pd.json_normalize(json_obj)

    df.to_csv(csv_file, index=False, encoding='utf-8')

    uploadCSV(table_name, csv_file)


def weather_import(table_name, csv_file):
    payload = {
        "lat":"38.83",
        "lon":"-122.83",
        "dt":"1765454400",
        "appid":api_config["weather_key"]
    }
    r = requests.get(api_endpoints["weather"], params=payload)
    print(r.text)

    json_obj = json.loads(r.text)
    df = pd.json_normalize(json_obj)

    df.to_csv(csv_file, index=False, encoding='utf-8')

    uploadCSV(table_name, csv_file)

def census_import(table_name, csv_file):
    payload = {
        "get":"NAME,POP",
        "HISP":"0",
        "for":"county:*",
        "in":"state:*"
    }
    r = requests.get(api_endpoints["census"], params=payload)
    print(r.text)

    json_obj = json.loads(r.text)
    df = pd.json_normalize(json_obj)

    df.to_csv(csv_file, index=False, encoding='utf-8')

    uploadCSV(table_name, csv_file)


def uploadCSV(table_name, csv_file, sep=','):
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Open the CSV file and use copy_from
        with open(csv_file_path, 'r') as f:
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

