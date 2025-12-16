import pandas as pd

def safe_get(row,column):
    value = getattr(row, column, None)
    return None if pd.isna(value) else value