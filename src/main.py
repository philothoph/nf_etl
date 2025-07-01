from database.schema_creator import setup_database
from etl.extractor import read_csv_file, list_csv_files
from etl.transformer import transform_dates
from etl.loader import load_to_db
from utils.date_detector import detect_date_columns


def main():
    # Set up the database, creating the necessary schemas, tables, and user
    setup_database()

    csv_files = list_csv_files()
    for fname in csv_files:
        df = read_csv_file(fname)        
        if df is not None:
            date_list = detect_date_columns(df)
            df = transform_dates(df, date_list)        
            load_to_db(df, fname.removesuffix(".csv").upper())


if __name__ == "__main__":
    main()