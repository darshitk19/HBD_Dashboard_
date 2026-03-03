import os
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = urllib.parse.quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)

def get_counts_fast(table, rev_col="reviews_average"):
    with engine.connect() as conn:
        print(f"\nAnalyzing {table} for exact duplicates (11 business columns):")
        
        # 1. Total rows
        total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        
        # 2. Count of Unique Groups (all 11 columns)
        cols = [
            "name", "address", "website", "phone_number", 
            "reviews_count", rev_col, 
            "category", "subcategory", "city", "state", "area"
        ]
        group_cols = ", ".join([f"`{c}`" for c in cols])
        
        print(f"  Counting unique groups...")
        unique_groups = conn.execute(text(f"SELECT COUNT(*) FROM (SELECT 1 FROM {table} GROUP BY {group_cols}) t")).scalar()
        
        extra_dupes = total - unique_groups
        percentage = (extra_dupes / total * 100) if total > 0 else 0
        
        print(f"  Total Rows:       {total:,}")
        print(f"  Unique Records:   {unique_groups:,}")
        print(f"  Extra Duplicates: {extra_dupes:,} ({percentage:.2f}%)")
        
        return {"total": total, "unique": unique_groups, "extra": extra_dupes}

if __name__ == "__main__":
    # We'll just do Raw for now as requested or common context
    # But let's do all 3 if fast enough
    try:
        raw = get_counts_fast("raw_google_map_drive_data", "reviews_average")
        clean = get_counts_fast("raw_clean_google_map_data", "reviews_avg")
        master = get_counts_fast("g_map_master_table", "reviews_avg")
    except Exception as e:
        print(f"Error: {e}")
