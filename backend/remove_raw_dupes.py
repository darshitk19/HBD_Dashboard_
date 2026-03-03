import sys, os, time
sys.stdout = open(sys.stdout.fileno(), 'w', encoding='utf-8')
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN',''))}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','3306')}/{os.getenv('DB_NAME')}",
    pool_pre_ping=True, pool_size=1, max_overflow=0
)

start = time.time()
total_deleted = 0

print("Step 1: Creating TEMP table for Keep IDs...")
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS _temp_keep_ids"))
    conn.execute(text("CREATE TABLE _temp_keep_ids (keep_id BIGINT PRIMARY KEY) ENGINE=InnoDB"))

print("Step 2: Populating Keep IDs (using READ UNCOMMITTED to avoid locks)...")
sys.stdout.flush()

# Use READ UNCOMMITTED to scan without waiting for locks from other transactions
# This is safe because we only care about existing IDs to keep them
with engine.begin() as conn:
    conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
    # Increase lock timeout for this specific operation
    conn.execute(text("SET SESSION innodb_lock_wait_timeout = 300"))
    conn.execute(text("""
        INSERT INTO _temp_keep_ids (keep_id)
        SELECT MIN(id)
        FROM raw_google_map_drive_data
        GROUP BY COALESCE(name,''), COALESCE(phone_number,''), COALESCE(city,''), COALESCE(address,'')
    """))

with engine.connect() as conn:
    keep_count = conn.execute(text("SELECT COUNT(*) FROM _temp_keep_ids")).fetchone()[0]
    total_count = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).fetchone()[0]
    to_delete = total_count - keep_count

print(f"Total rows: {total_count:,} | Keep: {keep_count:,} | To delete: {to_delete:,}")
print(f"Step 2 done in {time.time()-start:.0f}s")
sys.stdout.flush()

if to_delete <= 0:
    print("No duplicates found. Exiting.")
    sys.exit(0)

print(f"\nStep 3: Deleting {to_delete:,} duplicates in small batches...")
# Batch delete to minimize locking and disk pressure
BATCH_SIZE = 5000 
deleted_in_last_cycle = 0

while True:
    try:
        with engine.begin() as conn:
            # Short timeout here to stay out of Celery's way
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 5"))
            
            # Subquery join is usually faster for large deletes on primary keys
            result = conn.execute(text("""
                DELETE r FROM raw_google_map_drive_data r
                LEFT JOIN _temp_keep_ids k ON r.id = k.keep_id
                WHERE k.keep_id IS NULL
                LIMIT :batch
            """), {"batch": BATCH_SIZE})
            
            rowcount = result.rowcount
            total_deleted += rowcount
            
            if rowcount == 0:
                break
                
            elapsed = time.time() - start
            speed = total_deleted / elapsed if elapsed > 0 else 0
            remaining = to_delete - total_deleted
            eta_min = (remaining / speed / 60) if speed > 0 else 0
            
            if total_deleted % 25000 == 0 or rowcount < BATCH_SIZE:
                print(f"  Deleted: {total_deleted:,}/{to_delete:,} | Elapsed: {elapsed:.0f}s | ETA: {eta_min:.1f}m")
                sys.stdout.flush()
                
            time.sleep(0.05) # Tiny pause to yield to other DB operations
            
    except Exception as e:
        err = str(e)
        if 'Lock wait timeout' in err:
            time.sleep(2)
            continue
        elif 'No space left' in err:
            print("DISK FULL - Waiting 60s...")
            time.sleep(60)
            continue
        else:
            print(f"Error during delete: {err[:200]}")
            break

# Final Cleanup
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS _temp_keep_ids"))

print("\n" + "=" * 70)
print(f"COMPLETED! Removed {total_deleted:,} exact duplicates.")
print(f"Total Time: {time.time()-start:.1f}s")
with engine.connect() as conn:
    final_count = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).fetchone()[0]
    print(f"Final Row Count in Raw Table: {final_count:,}")
print("=" * 70)
