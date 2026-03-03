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

start_time = time.time()
print("Step 1: Fetching identifiers from database (streaming to save memory)...")
sys.stdout.flush()

seen = set()
to_delete = []

# Fetch data in chunks to be memory efficient
CHUNK_SIZE = 100000

try:
    with engine.connect() as conn:
        # We use a simple select. Ordering by ID ensures we keep the OLDEST row.
        res = conn.execution_options(stream_results=True).execute(text("""
            SELECT id, name, phone_number, city, address 
            FROM raw_google_map_drive_data 
            ORDER BY id ASC
        """))
        
        count = 0
        for row in res:
            count += 1
            # Create a signature
            sig = (
                (row[1] or "").strip().lower(),
                (row[2] or "").strip().lower(),
                (row[3] or "").strip().lower(),
                (row[4] or "").strip().lower()
            )
            
            if sig in seen:
                to_delete.append(row[0])
            else:
                seen.add(sig)
            
            if count % CHUNK_SIZE == 0:
                print(f"  Scanned {count:,} rows... Found {len(to_delete):,} duplicates")
                sys.stdout.flush()

    print(f"Step 1 Complete. Total Rows: {count:,} | Duplicates to remove: {len(to_delete):,}")
    print(f"Time taken: {time.time() - start_time:.1f}s")
    sys.stdout.flush()

    if not to_delete:
        print("No duplicates found. Exiting.")
        sys.exit(0)

    # Step 2: Delete in batches
    print(f"\nStep 2: Deleting {len(to_delete):,} duplicates in batches...")
    sys.stdout.flush()

    DELETE_BATCH = 5000
    total_removed = 0
    
    for i in range(0, len(to_delete), DELETE_BATCH):
        batch = to_delete[i:i + DELETE_BATCH]
        
        for attempt in range(5):
            try:
                with engine.begin() as conn:
                    # Short lock timeout to play nice with other workers
                    conn.execute(text("SET SESSION innodb_lock_wait_timeout = 5"))
                    conn.execute(text("DELETE FROM raw_google_map_drive_data WHERE id IN :ids"), {"ids": batch})
                
                total_removed += len(batch)
                break
            except Exception as e:
                if 'Lock wait timeout' in str(e):
                    time.sleep(2)
                    continue
                else:
                    print(f"  Error in delete batch: {e}")
                    break
        
        if total_removed % (DELETE_BATCH * 10) == 0:
            pct = (total_removed / len(to_delete)) * 100
            print(f"  Removed {total_removed:,}/{len(to_delete):,} | {pct:.1f}%")
            sys.stdout.flush()
        
        time.sleep(0.05) # Yield to DB

    print("\n" + "=" * 70)
    print(f"SUCCESS! Removed {total_removed:,} exact duplicates from raw_google_map_drive_data.")
    print(f"Final Execution Time: {time.time() - start_time:.1f}s")
    print("=" * 70)

except Exception as e:
    print(f"FATAL ERROR: {e}")
    sys.exit(1)
