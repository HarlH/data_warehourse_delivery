import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def ingest_data(file_path:str, table_name:str, engine):
    """
    Reads a CSV file and ingests the data into a table in PostgreSQL

    Args:
        file_path (str): Path to the CSV file.
        table_name (str): Name of the table in the database.
        engine: SQLAlchemy engine object for database connection.
    """
    try:
        logger.info(f"Reading file: {file_path}")
        df = pd.read_csv(f"{file_path}", encoding='latin-1')
        logger.info(f"Data read successfully. Rows: {len(df)}")

        logger.info(f"Inserting data into table '{table_name}'")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Ingestion of {table_name} completed successfully!")

    except FileNotFoundError:
        logger.error(f"Error: File not found at '{file_path}'.")
    except Exception as e:
        logger.error(f"Error in ingestion of '{table_name}' from '{file_path}': {e}", exc_info=True)

if __name__=='__main__':
    logger.info("Starting the data ingestion process...")
    
    load_dotenv()

    USER = os.environ.get('DB_USER')
    PASSWORD = os.environ.get('DB_PASSWORD')
    HOST = os.environ.get('DB_HOST')
    DB = os.environ.get('DB_NAME')

    try:
        logger.info(f"Connecting to database: {HOST}/{DB}")
        psql_engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:5432/{DB}")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")

    tables = [
        {'file_path': 'data/raw/channels.csv', 'table_name': 'raw_channels'},
        {'file_path': 'data/raw/deliveries.csv', 'table_name': 'raw_deliveries'},
        {'file_path': 'data/raw/drivers.csv', 'table_name': 'raw_drivers'},
        {'file_path': 'data/raw/hubs.csv', 'table_name': 'raw_hubs'},
        {'file_path': 'data/raw/orders.csv', 'table_name': 'raw_orders'},
        {'file_path': 'data/raw/payments.csv', 'table_name': 'raw_payments'},
        {'file_path': 'data/raw/stores.csv', 'table_name': 'raw_stores'}
    ]

    for table in tables:
        ingest_data(table['file_path'], table['table_name'], psql_engine)
    
    psql_engine.dispose()
    logger.info(f"Data ingestion process completed.")