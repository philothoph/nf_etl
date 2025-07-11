from database.schema_creator import run_sql_file
import os
import time

SQL_FOLDER = os.path.join(os.path.dirname(__file__), "..", "..", "sql")

def start_procedures():
    sql_files = [f for f in os.listdir(SQL_FOLDER) if f.endswith(".sql") and f.startswith("start_")]
    
    for f in sql_files:
        run_sql_file(f)