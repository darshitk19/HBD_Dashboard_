import os
import sys
import time
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)

def check_and_clean():
    start = time.time()
    print("="*60)
    print("FINAL AUDIT: EXACT DUPLICATE REMOVAL (11 COLUMNS)")
    print("="*60)
    
    with engine.connect() as conn:
        # Total rows now
        total_before = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).scalar()
        print(f"Current Raw Records: {total_before:,}")

        # The Duplicates Query
        # We group by all 11 columns to find sets that appear more than once
        print("Scanning for duplicates (all 11 columns)...")
        
        # We'll use a more direct SQL approach to find IDs to DELETE
        # This subquery finds the MIN(id) for every unique set
        # Any ID NOT in this set is a duplicate to be removed
        
        # NOTE: This is heavy on a 4M row table without a full covering index.
        # We'll use a temporary table to make it faster and safer.
        conn.execute(text("DROP TABLE IF EXISTS tmp_ids_to_keep"))
        print("Step 1: Identifying unique records to keep...")
        conn.execute(text("""
            CREATE TEMPORARY TABLE tmp_ids_to_keep (id BIGINT PRIMARY KEY)
            SELECT MIN(id) as id
            FROM raw_google_map_drive_data
            GROUP BY 
                name, LEFT(address, 255), LEFT(website, 255), phone_number, 
                reviews_count, reviews_average, 
                category, subcategory, city, state, area
        """))
        
        keep_count = conn.execute(text("SELECT COUNT(*) FROM tmp_ids_to_keep")).scalar()
        dupes_to_remove = total_before - keep_count
        
        print(f"  - Unique Records: {keep_count:,}")
        print(f"  - Extra Duplicates: {dupes_to_remove:,}")

        if dupes_to_remove > 0:
            print(f"Step 2: Deleting {dupes_to_remove:,} redundant rows...")
            # Delete rows where ID is NOT in the keep list
            # We do this in batches to avoid locking the database for too long
            
            # Since it's a temp table, we'll fetch IDs to delete into Python in chunks
            # so we can use a simple DELETE WHERE ID IN (...)
            
            # Get IDs that exist in raw but NOT in tmp_ids_to_keep
            sql_get_dupes = text("""
                SELECT r.id FROM raw_google_map_drive_data r
                LEFT JOIN tmp_ids_to_keep t ON r.id = t.id
                WHERE t.id IS NULL
            """)
            
            dupe_ids = [r[0] for r in conn.execute(sql_get_dupes)]
            
            BATCH = 5000
            for i in range(0, len(dupe_ids), BATCH):
                batch = dupe_ids[i:i+BATCH]
                with engine.begin() as trans_conn:
                    trans_conn.execute(text("DELETE FROM raw_google_map_drive_data WHERE id IN :ids"), {"ids": batch})
                if (i + BATCH) % 50000 == 0 or (i + BATCH) >= len(dupe_ids):
                    print(f"    - Removed {min(i+BATCH, len(dupe_ids)):,}/{len(dupe_ids):,} rows...")
        else:
            print("No more exact duplicates found!")

        total_after = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).scalar()
        print(f"\nFinal Raw Record Count: {total_after:,}")
        print(f"Total Cleanup Time: {time.time() - start:.1f}s")
        print("="*60)

if __name__ == "__main__":
    check_and_clean()
