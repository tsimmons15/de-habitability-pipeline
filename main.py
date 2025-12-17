from lib.logger import setup_logger
from ingestion.ingestion import start as ingestion_start
from cleaning.cleaning import start as cleaning_start
from transforming.transforming import start as transforming_start

import sys


logger = setup_logger("main_workflow", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s")

def main():
    print(f"sys.argv[0] = {sys.argv[0]}")
    if len(sys.argv) < 2:
        logger.error("Please pass in arguments indicating which stage of processing you're trying to achieve.")
        sys.exit(2)

    match(sys.argv[1]):
        case "ingestion":
            logger.info("Beginning ingestion...")
            ingestion_start()
        case "cleaning":
            logger.info("Beginning cleaning...")
            cleaning_start()
        case "transforming":
            logger.info("Beginning transforming...")
            transforming_start()
        case _:
            logger.warn("Unknown option. Valid options: ingestion, cleaning or transforming")

if __name__ == "__main__":
    main()
