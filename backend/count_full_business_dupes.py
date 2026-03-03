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

# Use COALESCE on all columns to handle NULL values
# name, address, website, phone_number, reviews_count, reviews_average, category, subcategory, city, state, area

def get_counts(table, rev_col="reviews_average"):
    with engine.connect() as conn:
        print(f"\nAnalyzing table: {table}")
        
        # Check total rows
        total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        print(f"  Total records: {total_rows:,}")

        # Columns to group by
        cols = [
            "name", "address", "website", "phone_number", 
            "reviews_count", rev_col, 
            "category", "subcategory", "city", "state", "area"
        ]
        
        # Use full column grouping to be 100% accurate
        # Since these are text/varchar, empty strings and NULLs can be tricky
        # MySQL's GROUP BY handles NULL as one group, which is normally what "exact duplicate" implies
        group_cols = ", ".join([f"`{c}`" for c in cols])
        
        # Group count where groups have > 1 row
        sql_groups = f"""
            SELECT COUNT(*) FROM (
                SELECT 1 FROM {table}
                GROUP BY {group_cols}
                HAVING COUNT(*) > 1
            ) t
        """
        
        # Sum (count - 1) to get the number of "extra" duplicates
        sql_extra = f"""
            SELECT COALESCE(SUM(cnt - 1), 0) FROM (
                SELECT COUNT(*) as cnt FROM {table}
                GROUP BY {group_cols}
                HAVING cnt > 1
            ) t
        """
        
        print(f"  Calculating (this may take a minute due to 4M+ rows and no index on all 11 columns)...")
        # Optimization: use row_signature for raw table ONLY if it's there
        if table == "raw_google_map_drive_data":
            # Check if row_signature is populated
            check_sig = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data WHERE row_signature IS NULL")).scalar()
            if check_sig == 0:
                print(f"  (Optimized using row_signature index/column)")
                sql_groups = f"SELECT COUNT(*) FROM (SELECT 1 FROM {table} GROUP BY row_signature HAVING COUNT(*) > 1) t"
                sql_extra = f"SELECT COALESCE(SUM(cnt - 1), 0) FROM (SELECT COUNT(*) as cnt FROM {table} GROUP BY row_signature HAVING cnt > 1) t"

        num_groups = conn.execute(text(sql_groups)).scalar()
        num_extra = conn.execute(text(sql_extra)).scalar()
        
        print(f"  Duplicate Groups: {num_groups:,}")
        print(f"  Extra rows to remove: {num_extra:,}")
        return {
            "total": total_rows,
            "groups": num_groups,
            "extra": num_extra
        }

if __name__ == "__main__":
    raw = get_counts("raw_google_map_drive_data", "reviews_average")
    clean = get_counts("raw_clean_google_map_data", "reviews_avg")
    master = get_counts("g_map_master_table", "reviews_avg")
    
    print("\n" + "="*50)
    print(f"{'Table':<30} | {'Total':>10} | {'Extra Dupes':>15}")
    print("-"*60)
    print(f"{'raw_google_map_drive_data':<30} | {raw['total']:>10,} | {raw['extra']:>15,}")
    print(f"{'raw_clean_google_map_data':<30} | {clean['total']:>10,} | {clean['extra']:>15,}")
    print(f"{'g_map_master_table':<30} | {master['total']:>10,} | {master['extra']:>15,}")
    print("="*50)
