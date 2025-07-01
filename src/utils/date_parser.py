import pandas as pd

DATE_FORMATS = ["%d.%m.%Y", "%d-%m-%Y", "%Y-%m-%d"]

def parse_date(value):
    """
    Parse various date strings into a pandas.Timestamp normalized to midnight,
    but return None on failure
    """
    for fmt in DATE_FORMATS:
        ts = pd.to_datetime(value, format=fmt, errors="coerce")
        if pd.isna(ts):
            continue
        return ts.normalize()
    return None
