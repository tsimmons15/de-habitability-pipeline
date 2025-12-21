from lib.logger import setup_logger

import psycopg2


logger = setup_logger("ingestion_library", "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",debug=True)

storage_directory = f"{os.environ.get('final_storage')}"
cleaning_directory = f"{storage_directory}/silver"


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
                    WHERE update_tm > (
                        select max(last_updated) 
                        from tsimmons.metadata
                        )
                    ) 
                    TO STDOUT 
                    WITH CSV DELIMITER ','
                """
        
        with open(f"{cleaning_directory}/input/{table_name}.csv", "w") as fh:
            cursor.copy_expert(sql, fh)
    
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def updateCleaning():
    logger.info("Replacing the last stage's data from PostgreSQL with the cleaned data...")
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        sql = f"""
                COPY tsimmons.{table_name} (
                    {cols}
                )
                FROM STDIN
                WITH (
                    FORMAT csv,
                    DELIMITER ',',
                    QUOTE '"',ESCAPE '\\')
               """
        with open(f"{cleaning_directory}/input/{table_name}.csv", "w") as fh:
            cursor.copy_expert(sql, fh)

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

def call_insert_truncate():
    logger.info("Truncating the insert tables.")
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Prepare the stored procedure.
        call = "CALL truncate_silver_insert_tables()"
        cursor.execute(call)
        conn.commit()
    except Exception as e:
        logger.error(f"Error: {e}")
        conn.rollback()
        raise e
    finally:
        if conn:
            cursor.close()
            conn.close()

def call_merge_raw():
    logger.info("Truncating the insert tables.")
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Prepare the stored procedure.
        call = "CALL merge_silver_insert_raw()"
        cursor.execute(call)
        conn.commit()
    except Exception as e:
        logger.error(f"Error: {e}")
        conn.rollback()
        raise e
    finally:
        if conn:
            cursor.close()
            conn.close()

def normalize(string):
    return string.lower().replace(" ", "")


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

