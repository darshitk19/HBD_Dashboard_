import sys, os, time
sys.stdout = open(sys.stdout.fileno(), 'w', encoding='utf-8')
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN',''))}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','3306')}/{os.getenv('DB_NAME')}",
    pool_pre_ping=True
)

# This script is designed to be RE-RUNNABLE. 
# It works in ID windows to avoid hitting timeouts.

start_time = time.time()
total_deleted = 0

with engine.connect() as conn:
    max_id = conn.execute(text("SELECT MAX(id) FROM raw_google_map_drive_data")).fetchone()[0]
    min_id = conn.execute(text("SELECT MIN(id) FROM raw_google_map_drive_data")).fetchone()[0]

# WINDOW_SIZE of 25,000 should finish within 30-60 seconds even with JOIN
WINDOW_SIZE = 25000
current_start = min_id

print(f"Starting Incremental Deduplication (Window: {WINDOW_SIZE:,} rows)")
print(f"ID Range: {min_id:,} to {max_id:,}")
sys.stdout.flush()

while current_start <= max_id:
    current_end = current_start + WINDOW_SIZE
    
    try:
        with engine.begin() as conn:
            # Set a very short lock timeout to avoid blocking others
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 10"))
            
            # The heart: self-join delete on a limited window
            # This is slow but GUARANTEES consistency.
            # Using t2.id < t1.id ensures we keep the oldest record.
            result = conn.execute(text("""
                DELETE t1 FROM raw_google_map_drive_data t1
                INNER JOIN raw_google_map_drive_data t2
                ON t1.id > t2.id
                AND COALESCE(t1.name,'') = COALESCE(t2.name,'')
                AND COALESCE(t1.phone_number,'') = COALESCE(t2.phone_number,'')
                AND COALESCE(t1.city,'') = COALESCE(t2.city,'')
                AND COALESCE(t1.address,'') = COALESCE(t2.address,'')
                WHERE t1.id >= :start AND t1.id < :end
            """), {"start": current_start, "end": current_end})
            
            deleted = result.rowcount
            total_deleted += deleted
            
            elapsed = time.time() - start_time
            pct = ((current_end - min_id) / (max_id - min_id)) * 100
            print(f"  Window {current_start:,}-{current_end:,} | Deleted: {deleted} | Total: {total_deleted:,} | {pct:.1f}% | {elapsed:.0f}s")
            sys.stdout.flush()
            
    except Exception as e:
        print(f"  Error in window {current_start}: {str(e)[:100]}")
        sys.stdout.flush()
        time.sleep(1)

    current_start = current_end
    # Small pause to let other queries in
    time.sleep(0.1)

print("\n" + "=" * 70)
print(f"FINISHED! Removed {total_deleted:,} duplicates.")
print(f"Total Time: {time.time()-start_time:.1f}s")
print("=" * 70)
