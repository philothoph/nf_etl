from database.schema_creator import setup_database
from etl.extractor import read_csv_file, list_csv_files
from etl.transformer import transform_dates
from etl.loader import load_to_db
from utils.date_detector import detect_date_columns
from utils.get_primary_keys import get_primary_keys
from database.connection import db_session
from database.logger_db import log_file_processing
import time

def main():
    # Set up the database, creating the necessary schemas, tables
    setup_database()
    

    with db_session() as conn:
        # Read and process CSV files
        csv_files = list_csv_files()
        for fname in csv_files:
            with log_file_processing(conn, fname):
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
                    load_to_db(df, fname.removesuffix(".csv").upper())
                time.sleep(5)


if __name__ == "__main__":
    main()