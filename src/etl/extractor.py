import os
import pandas as pd
from pathlib import Path

CSV_FOLDER = os.path.join(os.path.dirname(__file__), "..", "..", "data")
ENCODINGS = ["utf-8", "cp1251", "latin-1", "iso-8859-1"]

def list_csv_files():
    # Lists all CSV files in the directory.
    """Lists all CSV files in the directory.

    Returns:
        list: A list of CSV filenames.
    """
    return [f for f in os.listdir(CSV_FOLDER) if f.endswith(".csv")]

def read_csv_file(filename):
    """
    Reads a CSV file from the data folder and returns a pandas DataFrame.

    Args:
        filename (str): Name of the CSV file to read.

    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
    file_path = Path(CSV_FOLDER) / filename

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    for enc in ENCODINGS:
        try:
            df = pd.read_csv(file_path, sep=";", encoding=enc)
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Error reading '{filename}': {e}")
            return None
    print(f"All encoding attempts failed for file: {filename}")
    return None
