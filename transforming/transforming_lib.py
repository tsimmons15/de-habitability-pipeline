from lib.logger import setup_logger

import psycopg2


logger = setup_logger("ingestion_library", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",debug=True)





def getData(table_name):
    logger.info("Pulling the last stage's data down from PostgreSQL...")
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        sql = "COPY (SELECT * FROM table_name WHERE month=6) TO STDOUT WITH CSV DELIMITER ';'"
        with open("", "w") as file:
            cursor.copy_expert(sql, file)

        # Commit the transaction
        conn.commit()

    except Exception as e:
        logger.error(f"Error: {e}")
        if conn:
            conn.rollback()
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

