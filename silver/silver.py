from silver_lib import createSpark

hdfs_path = "UKUS18nov/tsimmons/project/bronze"

spark = createSpark()

# Load in the data to the dataframes
usgs_df = spark.read.parquet(f"{hdfs_path}/usgs_raw.parquet")
geocode = spark.read.parquet(f"{hdfs_path}/geocode_raw.parquet")
census = spark.read.parquet(f"{hdfs_path}/census_raw.parquet")
weather = spark.read.parquet(f"{hdfs_path}/weather_raw.parquet")

# Remove duplicate rows
usgs_df.dropDuplicates()
geocode.dropDuplicates()
census.dropDuplicates()
weather.dropDuplicates()

# Make sure the dates are filled in, using the epoch date if missing/null
# Don't filter out the NAs; they can still be used to report averages, though if
# one of 
usgs_df = usgs_df.na.fill({"time": datetime.fromtimestamp(0)})
usgs_df = usgs_df.na.fill({"updated": datetime.fromtimestamp(0)})
geocode_df = geocode_df.na.fill({"dt":datetime.fromtimestamp(0)})
census_df = census_df.na.fill({"dt":datetime.fromtimestamp(0)})
weather_df = weather_df.na.fill({"date":datetime.fromtimestamp(0)})

# Weather["date"] is just a date stamp, for consistency's sake make it a datetime stamp.
weather_df = weather_df.withColumn("date", datetime.strptime(col("date"), "YYYY-mm-DD"))

# Make the latitudes/longitudes consistent. The dataset doesn't have a lot of
#  precision, so losing some is not a massive issue.
usgs_df = usgs_df.withColumn("longitude", round(col("longitude"), 5))
usgs_df = usgs_df.withColumn("latitude", round(col("latitude"), 5))

weather_df = weather_df.withColumn("latitude", round(col("lat"), 5))
weather_df = weather_df.withColumn("longitude", round(col("lon"), 5))

geocode_df = geocode_df.withColumn("latitude", round(col("latitude"), 5))
geocode_df = geocode_df.withColumn("longitude", round(col("longitude"), 5))

# Make sure all of the aggregated columns are filled in, setting them to negative values so they'll be filtered out
#weather_result["cloud_cover"] = json_obj["cloud_cover"]["afternoon"]
#weather_result["humidity"] = json_obj["humidity"]["afternoon"]
#weather_result["precipitation"] = json_obj["precipitation"]["afternoon"]
#weather_result["pressure"] = json_obj["pressure"]["afternoon"]
#weather_result["temperature_max"] = json_obj["temperature"]["max"]
#weather_result["temperature_min"] = json_obj["temperature"]["min"]
#weather_result["wind"] = json_obj["wind"]["max"]["speed"]


# A lot of aggregates that use the count of items will be ran on the end-data, so filter rows that have missing values
weather_df = weather_df.filter(("temperature_max" is None) | ("temperature_max" < 0))
weather_df = weather_df.filter(("temperature_min" is None) | ("temperature_min" < 0))


# The weather API uses Kelvin and other SI units for the reporting of units, so convert them to more human-readable formats (Celsius, etc...)
weather_df = weather_df.withColumn("temperature_max", col("temperature_max") - 273.15)
weather_df = weather_df.withColumn("temperature_min", col("temperature_min") - 273.15)
