import psycopg2

def get_max_length(conn, table, column, schema='DS'):
    sql = """
    SELECT character_maximum_length
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name   = %s
      AND column_name  = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table, column))
        result = cur.fetchone()
    # result == (length,) or (None,)
    return result[0]