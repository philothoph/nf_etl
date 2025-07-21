import sys
import pandas as pd
import math
from psycopg2.errors import StringDataRightTruncation, CardinalityViolation
from psycopg2.extras import execute_values

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.database.connection import db_session
from src.utils.get_primary_keys import get_primary_keys

def load_to_db(df: pd.DataFrame, table_name: str, chunk_size: int = 5000, schema: str = 'DS'):
    """
    Loads a cleaned DataFrame into a PostgreSQL table with chunking for large datasets
    
    Args:
        df: DataFrame whose columns exactly match the target table.
        table_name: Name of the destination table.
        chunk_size: Number of rows to process in each chunk (default: 5000)
        schema: Schema of the destination table (default: 'DS')
    """
    if df.empty:
        print("DataFrame is empty - nothing to load.")
        return

    # Replace pandas values with None so psycopg2 writes SQL NULL
    df_clean = df.where(pd.notnull(df), None)
    primary_keys = get_primary_keys(table_name, schema=schema)
    print(f"Loading {len(df_clean)} rows into '{schema}.{table_name}' in chunks of {chunk_size}...")

    cols = ", ".join(df_clean.columns)
    update_columns = [c for c in df.columns if c not in primary_keys]
    total_rows = len(df_clean)
    num_chunks = math.ceil(total_rows / chunk_size)

    with db_session() as conn:
        with conn.cursor() as cur:
            try:
                # Truncate only once at the beginning if no primary keys (full replace)
                if not primary_keys:
                    print(f"Truncating table (no primary keys - full replace mode) {schema}.{table_name}")
                    cur.execute(f'TRUNCATE "{schema}"."{table_name}"')

                rows_processed = 0
                for chunk_idx in range(num_chunks):
                    start_idx = chunk_idx * chunk_size
                    end_idx = min((chunk_idx + 1) * chunk_size, total_rows)
                    chunk_df = df_clean.iloc[start_idx:end_idx]
                    records = chunk_df.values.tolist()

                    print(f"Processing chunk {chunk_idx + 1}/{num_chunks} ({len(records)} rows)")

                    if primary_keys:
                        _execute_upsert_chunk(cur, table_name, cols, records, primary_keys, update_columns, schema)
                    else:
                        _execute_insert_chunk(cur, table_name, cols, records, schema)

                    rows_processed += len(records)
                    conn.commit()

            except Exception as e:
                conn.rollback()
                print(f"Error during bulk load: {e}")
                raise

    print(f"Successfully loaded {rows_processed} rows into '{schema}.{table_name}'.")


def _execute_upsert_chunk(cur, table_name: str, cols: str, records: list, 
                         primary_keys: list, update_columns: list, schema: str):
    """Execute upsert for a single chunk with error handling"""
    set_sql = ", ".join(f'{c} = EXCLUDED.{c}' for c in update_columns)
    upsert_sql = f'''
        INSERT INTO "{schema}"."{table_name}" ({cols}) 
        VALUES %s 
        ON CONFLICT ({", ".join(primary_keys)}) 
        DO UPDATE SET {set_sql}
    '''
    
    try:
        execute_values(cur, upsert_sql, records, page_size=1000)
    except StringDataRightTruncation as e:
        print(f"StringDataRightTruncation error for table '{table_name}'")
        print(f"Column: {e.diag.column_name}")
        raise
    except CardinalityViolation:
        print("CardinalityViolation detected. Skipping record.")
        return


def _execute_insert_chunk(cur, table_name: str, cols: str, records: list, schema: str):
    """Execute insert for a single chunk with error handling"""
    insert_sql = f'INSERT INTO "{schema}"."{table_name}" ({cols}) VALUES %s'
    
    try:
        execute_values(cur, insert_sql, records, page_size=1000)
    except StringDataRightTruncation as e:
        print(f"StringDataRightTruncation error for table '{table_name}'")
        print(f"Column: {e.diag.column_name}")
        raise