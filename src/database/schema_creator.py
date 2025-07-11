import os
import sys

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.database.connection import db_session

SQL_FOLDER = os.path.join(os.path.dirname(__file__), "..", "..", "sql")

def run_sql_file(filename):
    """Run a SQL file.

    Args:
        filename: The name of the SQL file to run.

    Raises:
        FileNotFoundError: If the specified SQL file does not exist.
    """
    sql_path = os.path.join(SQL_FOLDER, filename)
    if not os.path.isfile(sql_path):
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as sql_file:
        sql = sql_file.read()

    with db_session() as db_connection:
        with db_connection.cursor() as db_cursor:
            db_cursor.execute(sql)

def setup_database():
    """
    Sets up the database by executing a list of SQL files.

    This function attempts to run several SQL scripts to create schemas, users, tables, and log tables
    necessary for the database initialization. If any SQL file execution fails, an error message is printed.

    Raises:
        Exception: If there is an error executing any of the SQL files.
    """

    sql_files = [f for f in os.listdir(SQL_FOLDER) if f.endswith(".sql") and f.startswith("create_")]


    for file in sql_files:
        try:
            run_sql_file(file)
        except Exception as e:
            print(f"Error running {file}: {e}")

