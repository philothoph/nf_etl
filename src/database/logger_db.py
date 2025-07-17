from contextlib import contextmanager
from datetime import datetime

@contextmanager
def log_file_processing(db_connection, file_name, action):
    """Simple context manager for logging file processing"""
    
    # Start logging
    start_time = datetime.now()
    
    query = """
    INSERT INTO "LOGS".etl_logs (start_time, file_name, action)
    VALUES (%s, %s, %s)
    RETURNING id
    """
    
    with db_connection.cursor() as cursor:
        cursor.execute(query, (start_time, file_name, action))
        log_id = cursor.fetchone()[0]
        db_connection.commit()
    
    print(f"Started processing: {file_name} at {start_time}")
    
    try:
        yield
        
    finally:
        # Always log end time (even if error occurred)
        end_time = datetime.now()
        
        query = """
        UPDATE "LOGS".etl_logs
        SET end_time = %s
        WHERE id = %s
        """
        
        with db_connection.cursor() as cursor:
            cursor.execute(query, (end_time, log_id))
            db_connection.commit()
        
        duration = (end_time - start_time).total_seconds()
        print(f"Finished processing: {file_name} in {duration:.2f} seconds")