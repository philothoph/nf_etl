from database.schema_creator import setup_database
from utils.start_procedures import start_procedures
from utils.load_to_csv import export_table_to_csv
from utils.etl import etl_process

def main():
    # Set up the database, creating the necessary schemas, tables
    setup_database()
    
    # Extract data from csv, transform and load to db 
    etl_process()

    # Run stored procedures
    start_procedures()

    # Export data to CSV file
    table_name = '"DM"."DM_F101_ROUND_F"'
    csv_name = "DM_F101_ROUND_F_v2" # name chosen to be loaded into the database later

    export_table_to_csv(table_name, csv_name)


if __name__ == "__main__":
    main()