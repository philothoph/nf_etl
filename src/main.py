def main():
    # 1. Initialize logging
    logger = setup_logging()
    
    # 2. Log ETL start
    log_etl_start()
    
    # 3. Add 5-second pause
    time.sleep(5)
    
    # 4. Process each CSV file
    for csv_file in csv_files:
        # Extract
        df = extract_csv(csv_file)
        
        # Transform
        df_clean = transform_data(df, table_name)
        
        # Load (upsert)
        load_to_db(df_clean, table_name)
    
    # 5. Log ETL completion
    log_etl_end()