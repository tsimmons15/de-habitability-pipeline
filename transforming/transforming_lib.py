from lib.logger import setup_logger

import psycopg2
from pyspark.sql.functions import col

logger = setup_logger("transformation_library", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",debug=True)

def getData(table_name):
    logger.info("Pulling the last stage's data down from PostgreSQL...")
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        sql = f"""COPY (
                    SELECT *
                    FROM {table_name}
                    TO STDOUT
                    WITH CSV DELIMITER ','
                """

        with open(f"{transforming_directory}/input/{table_name}.csv", "w") as fh:
            cursor.copy_expert(sql, fh)

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()


def parseConf():
    db_text = os.environ.get('db_conf')

    logger.info(f"The db_config text read in: {db_text}")
    if not db_text:
        logger.error("Unable to parse the db configuration environment variable. Please check.")
        sys.exit(4)

    global db_config
    db_config = json.loads(db_text)
    logger.info(f"The json parsed db_config: {db_config}")

db_config = {}

