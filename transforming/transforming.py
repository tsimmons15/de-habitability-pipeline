from lib.spark import createSpark
from lib.logger import setup_logger


hdfs_path = "UKUS18nov/tsimmons/project/bronze"

logger = setup_logger("main_transforming", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s") 

#Load in the data to the dataframes
def start():
    spark = createSpark()
