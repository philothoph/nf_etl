import pandas as pd
import sys

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.utils.date_parser import parse_date




def transform_dates(
    df: pd.DataFrame,
    date_columns: list[str]
):
    """
    Parse specified date columns in‐place using parse_date(), 
    returning the same DataFrame with those columns converted.

    Args:
        df: Input DataFrame.
        date_columns: List of column names to parse.

    Returns:
        The original DataFrame with parsed date columns.
    """
    for col in date_columns:
        if col not in df.columns:
            print(f"Warning: '{col}' not found; skipping.")
            continue

        # apply parse_date to each value; returns None on failure
        df[col] = df[col].apply(parse_date)

    return df