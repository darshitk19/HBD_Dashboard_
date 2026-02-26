"""
Safely removes duplicate rows from raw_google_map_drive_data.
Strategy: Delete in batches, keeping the row with the LOWEST id for each unique (name, phone_number, city, state).
Uses batch processing to avoid connection timeouts.
"""
import os
import time
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = urllib.parse.quote_plus(os.getenv('DB_PASSWORD') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    connect_args={'read_timeout': 600, 'write_timeout': 600}
)

BATCH_SIZE = 50000  # Delete 50k duplicates per batch

def get_count():
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
        return conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).scalar()

print("=" * 60)
print("  DATABASE DEDUPLICATION")
print("=" * 60)

before_count = get_count()
print(f"  Rows BEFORE: {before_count:,}")
print(f"  Strategy: Keep lowest ID per (name, phone_number, city, state)")
print(f"  Batch size: {BATCH_SIZE:,} rows per cycle")
print("=" * 60)

total_deleted = 0
round_num = 0

while True:
    round_num += 1
    print(f"\n--- Round {round_num} ---")
    
    try:
        with engine.begin() as conn:
            # Find duplicate IDs to delete in this batch
            # This keeps the MIN(id) and marks all others for deletion
            result = conn.execute(text(f"""
                DELETE t1 FROM raw_google_map_drive_data t1
                INNER JOIN (
                    SELECT MIN(id) as keep_id, name, phone_number, city, state
                    FROM raw_google_map_drive_data
                    WHERE name IS NOT NULL
                    GROUP BY name, phone_number, city, state
                    HAVING COUNT(*) > 1
                    LIMIT {BATCH_SIZE}
                ) t2 ON t1.name = t2.name 
                    AND (t1.phone_number = t2.phone_number OR (t1.phone_number IS NULL AND t2.phone_number IS NULL))
                    AND (t1.city = t2.city OR (t1.city IS NULL AND t2.city IS NULL))
                    AND (t1.state = t2.state OR (t1.state IS NULL AND t2.state IS NULL))
                    AND t1.id != t2.keep_id
            """))
            
            deleted = result.rowcount
            total_deleted += deleted
            print(f"  Deleted: {deleted:,} rows (Total so far: {total_deleted:,})")
            
            if deleted == 0:
                print("\n✅ No more duplicates found!")
                break
                
    except Exception as e:
        print(f"  ⚠️ Error in round {round_num}: {e}")
        print("  Retrying in 5 seconds...")
        time.sleep(5)
        continue
    
    # Brief pause to let DB breathe
    time.sleep(1)

after_count = get_count()
print(f"\n{'=' * 60}")
print(f"  DEDUPLICATION COMPLETE")
print(f"  Rows BEFORE : {before_count:,}")
print(f"  Rows AFTER  : {after_count:,}")
print(f"  Removed     : {total_deleted:,} duplicates")
print(f"  Savings     : {(total_deleted/before_count*100):.1f}%")
print(f"{'=' * 60}")
