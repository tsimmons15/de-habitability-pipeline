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
usgs_df = usgs_df.na.fill({"dt": datetime.fromtimestamp(0)})
geocode_df = geocode_df.na.fill({"dt":datetime.fromtimestamp(0)})
census_df = census_df.na.fill({"dt":datetime.fromtimestamp(0)})
weather_df = weather_df.na.fill({"dt":datetime.fromtimestamp(0)})

# Make sure any of the aggregated columns are filled in, setting them to negative values so they'll be filtered out



# A lot of aggregates that use the count of items will be ran on the end-data, so filter rows that have missing values
weather_df = weather_df.filter((col("temperature_max") is None) | (col("temperature_max") < 0))
weather_df = weather_df.filter((col("temperature_min") is None) | (col("temperature_min") < 0))


# The weather API uses Kelvin and other SI units for the reporting of units, so convert them to more human-readable formats (Celsius, etc...)
weather_df = weather_df.withColumn(col("temperature_max"), col("temperature_max") - 273.15)
weather_df = weather_df.withColumn(col("temperature_min"), col("temperature_min") - 273.15)
