import psycopg2
from contextlib import contextmanager

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.database import DB_CONFIG

def get_connection():
    """
    Tries to connect to the database.

    Returns a database connection as a connection object if successful, or None if an error occurs.

    The connection is set to not autocommit (i.e., transactions are not automatically committed after each operation),
    so that we can manually commit or rollback operations with conn.commit() or conn.rollback().
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None
    
@contextmanager
def db_session():
    """
    Yields a database connection as a context manager.

    The connection is automatically rolled back if an exception occurs,
    and closed when the context is exited.

    Raises a ConnectionError if the connection cannot be established.
    """
    conn = get_connection()
    if conn is None:
        raise ConnectionError("Failed to connect to the database")
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()