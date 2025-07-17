import csv
from pathlib import Path
from database.connection import db_session
from database.logger_db import log_file_processing

def export_table_to_csv(table_name: str, csv_name: str):
    """
    Export data from a PostgreSQL table to a CSV file with logging.
    """
    try:
        csv_path = str(Path(__file__).parent.parent.parent / 'data' / f'{csv_name}.csv')
        with db_session() as conn:
            with log_file_processing(conn, csv_name, 'write'):
                cursor = conn.cursor()
                cursor.execute(f'SELECT * FROM {table_name}')
                rows = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]

                # ensure directory exists
                Path(csv_path).parent.mkdir(parents=True, exist_ok=True)

                with open(csv_path, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter=';')
                    writer.writerow(colnames)
                    for row in rows:
                        # handle zeros as 0-E8
                        writer.writerow(['0' if str(val) == '0E-8' else val for val in row])
                    
                print(f'Data exported to {csv_name} successfully.')

    except Exception as e:
        print(f'An error occurred: {e}')
