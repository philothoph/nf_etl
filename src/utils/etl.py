from etl.extractor import read_csv_file, list_csv_files
from etl.transformer import transform_dates
from etl.loader import load_to_db
from utils.date_detector import detect_date_columns
from utils.get_primary_keys import get_primary_keys
from database.connection import db_session
from database.logger_db import log_file_processing
from psycopg2.errors import UndefinedTable
import time

def etl_process():
    with db_session() as conn:
            # Read and process CSV files
            csv_files = list_csv_files()
            for fname in csv_files:
                with log_file_processing(conn, fname, 'read'):
                    df = read_csv_file(fname)        
                    if df is not None:
                        # Parse date columns
                        date_list = detect_date_columns(df)
                        df = transform_dates(df, date_list)
                        # Remove duplicates
                        primary_keys = get_primary_keys(fname.removesuffix(".csv").upper())
                        if primary_keys:
                            df = df.drop_duplicates(subset=primary_keys, keep="last")
                        # Load the DataFrame into the database
                        try:       
                            load_to_db(df, fname.removesuffix(".csv").upper())
                        except UndefinedTable:
                            load_to_db(df, fname.removesuffix(".csv").upper(), schema="DM")
                    time.sleep(5)