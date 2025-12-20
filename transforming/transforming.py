from lib.spark import createSpark
from lib.logger import setup_logger

logger = setup_logger("main_transforming", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s") 

#Load in the data to the dataframes
def start():
    spark = createSpark()

    logger.info("Loading the dataframes...")
    usgs_df = spark.read.parquet(f"{hdfs_path}/usgs_raw.parquet")
    geocode = spark.read.parquet(f"{hdfs_path}/geocode_raw.parquet")
    census = spark.read.parquet(f"{hdfs_path}/census_raw.parquet")
    weather = spark.read.parquet(f"{hdfs_path}/weather_raw.parquet")
