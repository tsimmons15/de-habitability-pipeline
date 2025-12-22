from lib.spark import createSpark
from lib.logger import setup_logger

from pyspark.sql.functions import coalesce, col, floor, lit, row_number, min, max, date_format, year, quarter, month, dayofmonth, weekofyear as week_of_year,date_trunc, last_day, explode 
from pyspark.sql.window import Window
from geopy.distance import great_circle

logger = setup_logger("main_transforming", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s") 

storage_directory = f"{os.environ.get('final_storage')}"
transformation_directory = f"{storage_directory}/gold"

#Load in the data to the dataframes
def start():
    if not storage_directory or not transformation_directory:
        raise Exception("Data storage location is missing. Unable to proceed.")
    if not os.path.exists(f"{transforming_directory}/input/"):
        raise Exception("Missing cleaning directory input, nothing to do...")
    if not os.path.exists(f"{transforming_directory}/output/"):
        os.makedirs(f"{transforming_directory}/output/")

    spark = createSpark()

    geo_bins = 0.7

    for table in tables:
        getData(table)


    logger.info("Loading the dataframes...")
    usgs_df = spark.read.csv(f"{transforming_directory}/input/usgs_raw.csv", header=True)
    geocode_df = spark.read.csv(f"{transforming_directory}/input/geocode_raw.cvs", header=True)
    census_df = spark.read.csv(f"{transforming_directory}/input/census_raw.cvs", header=True)
    weather_df = spark.read.csv(f"{transforming_directory}/input/weather_raw.cvs", header=True)




    # Prepare to join the geocode/census data, so I'll have a human-readable way to refer to lat/lon pairs:
    geocode = geocode_df.select(
        col("name_key"),
        col("name").alias("geocode_name"), # Coalesce will provide a means of combining the two name columns, so 
        col("lat").cast("double").alias("geo_lat"), # geo_lat/geo_lon will show up useful later in the joins/pairing with USGS/Weather data
        col("lon").cast("double").alias("geo_lon"),
        col("country"),
        col("state")
    )

    census = census_df.select(
        col("name_key"),
        col("name").alias("census_name"),
        col("pop").cast("long")
    )

    location_dimension = geocode.join(census, on="name_key", how="left").withColumn("area_name", coalesce("geocode_name", "census_name"))
    # Now that the two columns have been coalesced, they're not needed anymore.
    location_dimension.drop(["census_name", "geocode_name"])


    # Turn the lat/lon values into (effectively) an integer to give the join something easier to work on
    # geo_bins is a fudge factor to give some room for nearby lat/lon pairs to match.
    loc_binned = (location_dimension
        .withColumn("lat_bin", floor(col("geo_lat") / lit(geo_bins)))
        .withColumn("lon_bin", floor(col("geo_lon") / lit(geo_bins)))
    )

    weather_binned = (weather_df
        .withColumn("lat_bin", floor(col("lat") / lit(geo_bins)))
        .withColumn("lon_bin", floor(col("lon") / lit(geo_bins)))
    )

    usgs_binned = (usgs_df
        .withColumn("lat_bin", floor(col("latitude") / lit(geo_bins)))
        .withColumn("lat_bin", floor(col("longitude") / lit(geo_bins)))
    )

    weather_candidates = weather_binned.join(loc_binned, on=["lat_bin", "lon_bin"], how="left")
    
    # Using the haversine formula, which approximates earth as a circle to calculate distances between two points
    # This will be used in case there are multiple lat/lon for a given area, which shouldn't happen, but better safe than sorry
    weather_candidates = weather_candidates.withColumn("dist_miles", great_circle((col("lat"), col("lon")), (col("geo_lat"), col("geo_lon"))).miles)
    
    # Used in the determination of whether a row is the same as another, uses pyspark.sql.function functions to provide lazy evaluating
    weather_id = sha2(concat_ws("||", "date", "lat", "lon"), 256)
    # This is where the lazy evaluating becomes tied to a weather row.
    weather_candidates = weather_candidates.withColumn("weather_id", w_id)
    
    # Determines a rank of duplicate weather_id entries, if there are any
    w = Window.partitionBy("weather_id").orderBy(col("dist_miles").asc_nulls_last())
    # Filter out all but the closest distances using the rank
    weather_loc = (weather_candidates
        .withColumn("rnk", row_number().over(w))
        .filter(col("rnk") == 1)
        .drop("rnk")
    )
    
    # Do the same above for the USGS data, since place is not as reliable as lat/lon
    usgs_candidates = usgs_binned.join(loc_binned, on=["lat_bin", "lon_bin"], how="left")

    usgs_candidates = usgs_candidates.withColumn("dist_miles", great_circle((col("latitude"), col("longitude")), (col("geo_lat"), col("geo_lon"))).miles)

    usgs_id = sha2(concat_ws("||", "time", "latitude", "longitude"), 256)
    usgs_candidates = usgs_candidates.withColumn("usgs_id", usgs_id)

    w = Window.partitionBy("usgs_id").orderBy(col("dist_miles").asc_nulls_last())
    usgs_loc = (usgs_candidates
        .withColumn("rnk", row_number().over(w))
        .filter(col("rnk") == 1)
        .drop("rnk")
    )

    # Now that weather and usgs are both lat/loc tied to placenames with the same lat/lon source, join on the lat/lon to provide final table
    attribute_table = weather_loc.join(usgs_loc, on=['geo_lat', 'geo_lon'], how="left")

    date_bounds = fact_weather_df.select(
        min("date").alias("min_date"),
        max("date").alias("max_date")
    ).collect()[0]

    start_date = date_bounds["min_date"]
    end_date   = date_bounds["max_date"]

    dim_date = (
        spark
        .sql(f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) AS date")
        .select(explode("date").alias("date"))
    )

    dim_date = (
        dim_date
        .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", year("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("month", month("date"))
        .withColumn("month_name", date_format("date", "MMMM"))
        .withColumn("month_short", date_format("date", "MMM"))
        .withColumn("day", dayofmonth("date"))
        .withColumn("day_of_week", date_format("date", "u").cast("int"))  # 1=Mon
        .withColumn("day_name", date_format("date", "EEEE"))
        .withColumn("week_of_year", weekofyear("date"))
        .withColumn("iso_year", year(date_trunc("week", "date")))
        .withColumn("year_month", date_format("date", "yyyy-MM"))
        .withColumn("is_weekend", col("day_of_week").isin(6, 7))
        .withColumn("is_month_start", col("day") == 1)
        .withColumn("is_month_end", last_day("date") == col("date"))
        .withColumn("is_year_start", (month("date") == 1) & (col("day") == 1))
        .withColumn("is_year_end", (month("date") == 12) & (col("day") == 31))
    )




    # To write: dim_date, location_dimension, attribute_table
    attribute_table.write.csv(f"{transforming_directory}/output/attribute_table.csv", header=True)
    location_dimension.write.csv(f"{transforming_directory}/output/location_dimension.csv", header=True)
    dim_date.write.csv("{transforming_directory}/output/dim_date.csv", header=True)
