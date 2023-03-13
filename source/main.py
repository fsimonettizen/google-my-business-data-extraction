import sys
import postgres_config
import ddl_generator
import psycopg2
import gmb_extract_raw_data
# import logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

def lambda_handler(event, context):

    #rds settings
    db_host = postgres_config.db_host
    db_port = postgres_config.db_port
    db_username = postgres_config.db_username
    db_password = postgres_config.db_password
    db_name = postgres_config.db_name

    try:
        conn = psycopg2.connect(host=db_host, user=db_username, password=db_password, database=db_name, port=db_port, connect_timeout=5)
    except Exception as e:
        print("ERROR: Unexpected error: Could not connect to Postgres instance.")
        print(e)
        sys.exit()

    print("SUCCESS: Connection to RDS Postgres instance succeeded")

    if ddl_generator.ddl_validate(conn):
        # Valida tabelas e campos
        gmb_extract_raw_data.gmb_extract_data(conn, event)

    return { 
        'message': 'SUCCESS: GMB Data extracted'
    }