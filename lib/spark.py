from pyspark.sql import SparkSession


def createSpark():
    return SparkSession.builder \
        .appName("silver cleaning") \
        .getOrCreate()

