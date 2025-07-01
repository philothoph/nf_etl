import sys
from typing import List, Optional

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.database.connection import db_session

def get_primary_keys(
    table_name: str,
    schema: Optional[str] = "DS"
) -> List[str]:
    """
    Returns a list of primary key column names for the given table.

    Args:
        conn: an open psycopg2 connection
        table_name: name of the table
        schema: schema name (default "public")

    Returns:
        List of column names that form the primary key
    """
    sql = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema  = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = %s
      AND tc.table_name   = %s
    ORDER BY kcu.ordinal_position;
    """

    with db_session() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (schema, table_name))
            rows = cur.fetchall()
        # rows is list of 1-tuples like [(col1,), (col2,), ...]
        return [row[0].upper() for row in rows]