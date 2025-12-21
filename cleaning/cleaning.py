from lib.spark import createSpark
from lib.logger import setup_logger
from cleaning.cleaning_lib import getData, call_insert_truncate, call_merge_raw, normalize

import os, psycopg2

storage_directory = f"{os.environ.get('final_storage')}"
cleaning_directory = f"{storage_directory}/silver"

logger = setup_logger("main_cleaning", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s")

# Load in the data to the dataframes
def start():
    if not storage_directory or not cleaning_directory:
        raise Exception("Data storage location is missing. Unable to proceed.")
    if not os.path.exists(f"{cleaning_directory}/input/"):
        raise Exception("Missing cleaning directory input, nothing to do...")
    if not os.path.exists(f"{cleaning_directory}/output/"):
        os.makedirs(f"{cleaning_directory}/output/")

    call_insert_truncate()

    tables = ['weather_silver', 'geocode_silver', 'census_silver', 'usgs_silver']
    
    for table in tables:
        getData(table)

    spark = createSpark()

    logger.info("Loading the dataframes...")
    usgs_df = spark.read.csv(f"{cleaning_directory}/input/usgs_raw.csv", header=True)
    geocode = spark.read.csv(f"{cleaning_directory}/input/geocode_raw.csv", header=True)
    census = spark.read.csv(f"{cleaning_directory}/input/census_raw.csv", header=True)
    weather = spark.read.csv(f"{cleaning_directory}/input/weather_raw.csv", header=True)

    # Remove duplicate rows
    logger.info("Removing duplicates from usgs based on 'place' and 'time'")
    usgs_df.dropDuplicates(subsets=['place', 'time'])
    logger.info("Removing duplicates from geocode based on 'name', 'lat' and 'lon'")
    geocode.dropDuplicates(subset=['name', 'lat', 'lon'])
    logger.info("Removing duplicates from census based on 'name'")
    census.dropDuplicates(subset=['name'])
    logger.info("Removing duplicates from weather based on 'lat', 'lon' and 'date'")
    weather.dropDuplicates(subset=['lat', 'lon', 'date'])

    # Remove unneeded columns
    logger.info("Removing hisp, state, county from census...")
    census_df.drop('hisp', 'state', 'county')

    # Make sure the dates are filled in, using the epoch date if missing/null
    # Don't filter out the NAs; they can still be used to report averages, though if
    # one of things wanted is averages over specific time periods, the report will be wrong barring
    # enough data.
    logger.info("Setting empty date fields to epoch date.")
    usgs_df = usgs_df.na.fill({"time": datetime.fromtimestamp(0)})
    usgs_df = usgs_df.na.fill({"updated": datetime.fromtimestamp(0)})
    geocode_df = geocode_df.na.fill({"dt":datetime.fromtimestamp(0)})
    census_df = census_df.na.fill({"dt":datetime.fromtimestamp(0)})
    weather_df = weather_df.na.fill({"date":datetime.fromtimestamp(0)})

    # Weather["date"] is just a date stamp, for consistency's sake make it a datetime stamp.
    logger.info("Normalizing date formats.")
    weather_df = weather_df.withColumn("date", datetime.strptime(col("date"), "YYYY-mm-DD"))

    # Make the latitudes/longitudes consistent. The dataset doesn't have a lot of
    #  precision, so losing some is not a massive issue.
    logger.info("Normalizing the latitude/longitude precisions.")
    usgs_df = usgs_df.withColumn("longitude", round(col("longitude"), 2))
    usgs_df = usgs_df.withColumn("latitude", round(col("latitude"), 2))

    weather_df = weather_df.withColumn("latitude", round(col("lat"), 2))
    weather_df = weather_df.withColumn("longitude", round(col("lon"), 2))

    geocode_df = geocode_df.withColumn("latitude", round(col("latitude"), 2))
    geocode_df = geocode_df.withColumn("longitude", round(col("longitude"), 2))

    # A lot of aggregates that use the count of items will be ran on the end-data, so filter rows that have missing values
    logger.info("Filtering empty/negative temperatures.")
    weather_df = weather_df.filter(("temperature_max" is None) | ("temperature_max" < 0))
    weather_df = weather_df.filter(("temperature_min" is None) | ("temperature_min" < 0))


    # Add name_key, used to correlate USGS and census data
    logger.info("Adding name_key to USGS/Census, used to improve efficiency of the join in future steps")
    geocode_df.withColumn("name_key", normalize(col("name")))
    census_df.withColumn("name_key", normalize(col("name")))


    # The weather API uses Kelvin and other SI units for the reporting of units, so convert them to more human-readable formats (Celsius, etc...)
    logger.info("Converting Kelvin temperatures to Celsius to make reporting easier.")
    weather_df = weather_df.withColumn("temperature_max", col("temperature_max") - 273.15)
    weather_df = weather_df.withColumn("temperature_min", col("temperature_min") - 273.15)

    
    # Write the output to the cleaning output directory, ready to update the table.
    weather_df.write.csv(f"{cleaning_directory/output/weather.csv", header=True, mode="overwrite")
    usgs_df.write.csv(f"{cleaning_directory/output/usgs.csv", header=True, mode="overwrite")
    geocode_df.write.csv(f"{cleaning_directory/output/geocode.csv", header=True, mode="overwrite")
    census_df.write.csv(f"{cleaning_directory/output/census.csv", header=True, mode="overwrite")

    # Update the pulled data with the cleaned
    upload_data = [
        {'table_name':'weather_silver_insert', 'cols':'lat, lon, tz, date, units, cloud_cover, humidity, precipitation, temperature_min, temperature_max, pressure, wind'},
        {'table_name':'usgs_silver_insert', 'cols':'mag, place, time, updated, tz, felt, cdi, mmi, alert, status, tsunami, sig, net, code, nst, dmin, rms, gap, \"magType\", type, title, latitude, longitude'},
        {'table_name':'geocode_silver_insert', 'cols':'name, lat, lon, country, state'},
        {'table_name':'census_silver_insert', 'cols':'name, pop, hisp, state, county'},
    ]
    for data in upload_data:
        updateCleaning(data["table_name"], data["cols"])
    
   call_merge_raw() 
