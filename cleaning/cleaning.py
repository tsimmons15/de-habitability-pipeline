from lib.spark import createSpark
from lib.logger import setup_logger
from cleaning.cleaning_lib import getData()

import os

storage_directory = f"{os.environ.get('final_storage')}"
cleaning_directory = f"{storage_directory}/silver"

logger = setup_logger("main_cleaning", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s")

# Load in the data to the dataframes
def start():
    if not storage_directory or not cleaning_directory:
        raise Exception("Data storage location is missing. Unable to proceed.")
    if not os.path.exists(f"{cleaning_directory}/input/"):
        raise Exception("Missing cleaning directory input, nothing to do...")

    getData()

    spark = createSpark()

    logger.info("Loading the dataframes...")
    usgs_df = spark.read.parquet(f"{cleaning_directory}/input/usgs_raw.parquet")
    geocode = spark.read.parquet(f"{cleaning_directory}/input/geocode_raw.parquet")
    census = spark.read.parquet(f"{cleaning_directory}/input/census_raw.parquet")
    weather = spark.read.parquet(f"{cleaning_directory}/input/weather_raw.parquet")

    # Remove duplicate rows
    logger.info("Removing duplicates from usgs based on 'place' and 'time'")
    usgs_df.dropDuplicates(subsets=['place', 'time'])
    logger.info("Removing duplicates from geocode based on 'lat' and 'lon'")
    geocode.dropDuplicates(subset=['lat', 'lon'])
    logger.info("Removing duplicates from census based on 'name'")
    census.dropDuplicates(subset=['name'])
    logger.info("Removing duplicates from weather based on 'lat', 'lon' and 'date'")
    weather.dropDuplicates(subset=['lat', 'lon', 'date'])

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
    usgs_df = usgs_df.withColumn("longitude", round(col("longitude"), 5))
    usgs_df = usgs_df.withColumn("latitude", round(col("latitude"), 5))

    weather_df = weather_df.withColumn("latitude", round(col("lat"), 5))
    weather_df = weather_df.withColumn("longitude", round(col("lon"), 5))

    geocode_df = geocode_df.withColumn("latitude", round(col("latitude"), 5))
    geocode_df = geocode_df.withColumn("longitude", round(col("longitude"), 5))

    # A lot of aggregates that use the count of items will be ran on the end-data, so filter rows that have missing values
    logger.info("Filtering empty/negative temperatures.")
    weather_df = weather_df.filter(("temperature_max" is None) | ("temperature_max" < 0))
    weather_df = weather_df.filter(("temperature_min" is None) | ("temperature_min" < 0))


    # The weather API uses Kelvin and other SI units for the reporting of units, so convert them to more human-readable formats (Celsius, etc...)
    logger.info("Converting Kelvin temperatures to Celsius to make reporting easier.")
    weather_df = weather_df.withColumn("temperature_max", col("temperature_max") - 273.15)
    weather_df = weather_df.withColumn("temperature_min", col("temperature_min") - 273.15)


    # Finished with the cleaning, so store it the silver output folder.
    if not os.path.exists(f"{storage_directory}/transformation/input/"):
        os.makedirs(f"{storage_directory}/transformation/input/", exist_ok=True)
    
    
