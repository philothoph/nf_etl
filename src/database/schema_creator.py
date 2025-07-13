import os
import sys

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.database.connection import db_session

SQL_FOLDER = os.path.join(os.path.dirname(__file__), "..", "..", "sql")

def run_sql_file(filename):
    """Run a SQL file."""
    sql_path = os.path.join(SQL_FOLDER, filename)
    if not os.path.isfile(sql_path):
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as sql_file:
        sql = sql_file.read()

    with db_session() as db_connection, db_connection.cursor() as db_cursor:
        db_cursor.execute(sql)

def setup_database():
    """Sets up the database by executing a list of SQL files."""
    sql_files = [f for f in os.listdir(SQL_FOLDER) if f.endswith(".sql") and f.startswith("create_")]


    for file in sql_files:
        try:
            run_sql_file(file)
        except Exception as e:
            print(f"Error running {file}: {e}")

    run_sql_file("fill_table_31.sql")
