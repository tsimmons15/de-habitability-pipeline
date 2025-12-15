from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit, col




def createSpark():
    return SparkSession.builder \
        .appName("gold transformation") \
        .getOrCreate()



