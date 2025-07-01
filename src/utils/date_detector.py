def detect_date_columns_by_name(df):
    """
    Return all columns whose name contains 'date' (case-insensitive).
    """
    return [col for col in df.columns if 'date' in col.lower()]