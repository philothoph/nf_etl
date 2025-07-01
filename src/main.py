from database.schema_creator import setup_database
from etl.extractor import read_csv_file, list_csv_files
from etl.transformer import transform_dates
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
            print(df.head(2))  # Show preview of first 2 rows


    # # 1. Initialize logging
    # logger = setup_logging()
    
    # # 2. Log ETL start
    # log_etl_start()
    
    # # 3. Add 5-second pause
    # time.sleep(5)
    
    # # 4. Process each CSV file
    # for csv_file in csv_files:
    #     # Extract
    #     df = extract_csv(csv_file)
        
    #     # Transform
    #     df_clean = transform_data(df, table_name)
        
    #     # Load (upsert)
    #     load_to_db(df_clean, table_name)
    
    # # 5. Log ETL completion
    # log_etl_end()

if __name__ == "__main__":
    main()