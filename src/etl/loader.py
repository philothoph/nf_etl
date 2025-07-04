import sys
import pandas as pd
from psycopg2.errors import StringDataRightTruncation, CardinalityViolation
from psycopg2.extras import execute_values

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.database.connection import db_session
from src.utils.get_primary_keys import get_primary_keys

def load_to_db(df: pd.DataFrame, table_name: str):
    """
    Loads a cleaned DataFrame into a PostgreSQL table
    
    Args:
        df: DataFrame whose columns exactly match the target table.
        table_name: Name of the destination table.
    """
    if df.empty:
        print("DataFrame is empty - nothing to load.")
        return

    # Replace pandas values with None so psycopg2 writes SQL NULL
    df_clean = df.where(pd.notnull(df), None)
    primary_keys = get_primary_keys(table_name)
    records = df_clean.values.tolist()
    cols = ", ".join(df_clean.columns)
    print(f"Loading {len(records)} rows into '{table_name}'...")    
    update_columns = [c for c in df.columns if c not in primary_keys]
    set_sql = ", ".join(f'{c} = EXCLUDED.{c}' for c in update_columns)
    insert_sql = f"TRUNCATE \"DS\".\"{table_name}\"; INSERT INTO \"DS\".\"{table_name}\" ({cols}) VALUES %s"
    upsert_sql = f"INSERT INTO \"DS\".\"{table_name}\" ({cols}) VALUES %s ON CONFLICT ({", ".join(primary_keys)}) DO UPDATE SET {set_sql}"

    with db_session() as conn:
        with conn.cursor() as cur:
            try:
                if primary_keys:
                    execute_values(cur, upsert_sql, records)
                else:
                    execute_values(cur, insert_sql, records)
            except StringDataRightTruncation as e:
                print(f"StringDataRightTruncation error for table '{table_name}'")
                print(e.diag.column_name)
            except CardinalityViolation:
                conn.rollback()
                df_clean = df_clean.drop_duplicates(subset=primary_keys, keep="last")
                records = df_clean.values.tolist()
                execute_values(cur, upsert_sql, records)


                
        print(f"Loaded {len(records)} rows into '{table_name}'.")