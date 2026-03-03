import os, time, sys, threading
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
DATABASE_URI = f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN','') or '')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URI, pool_pre_ping=True)

# Keep the shell alive
def heartbeat():
    while True:
        time.sleep(30)
        print("  [SYSTEM] Background thread alive...")
        sys.stdout.flush()

threading.Thread(target=heartbeat, daemon=True).start()

print("Starting deduplication based on: name, address, phone_number, city")
start_time = time.time()

try:
    with engine.connect() as conn:
        print("Obtaining table range...")
        res = conn.execute(text("SELECT MIN(id), MAX(id) FROM raw_google_map_drive_data")).fetchone()
        min_id, max_id = res[0], res[1]

    if not min_id:
        print("Table is empty.")
        sys.exit(0)

    # Process in windows to manage RAM and keep output flowing
    RANGE_SIZE = 200000
    seen_signatures = set()
    total_found = 0
    total_deleted = 0
    
    current_start = min_id
    while current_start <= max_id:
        current_end = current_start + RANGE_SIZE
        print(f"  Scanning Range: {current_start:,} to {current_end:,}...")
        sys.stdout.flush()
        
        with engine.connect() as conn:
            # ONLY fetching the 4 identity fields + id
            rows = conn.execute(text("""
                SELECT id, name, address, phone_number, city
                FROM raw_google_map_drive_data
                WHERE id >= :start AND id < :end
                ORDER BY id ASC
            """), {"start": current_start, "end": current_end}).fetchall()
            
            to_delete_batch = []
            for row in rows:
                # signature: (name, address, phone_number, city)
                sig = tuple(str(val).strip().lower() if val is not None else "" for val in row[1:])
                
                if sig in seen_signatures:
                    to_delete_batch.append(row[0])
                else:
                    seen_signatures.add(sig)
            
            if to_delete_batch:
                total_found += len(to_delete_batch)
                # Surgical deletion in batches of 5000
                SUB_BATCH = 5000
                for i in range(0, len(to_delete_batch), SUB_BATCH):
                    sub = to_delete_batch[i:i+SUB_BATCH]
                    for attempt in range(3):
                        try:
                            with engine.begin() as dconn:
                                dconn.execute(text("SET SESSION innodb_lock_wait_timeout = 10"))
                                dconn.execute(text("DELETE FROM raw_google_map_drive_data WHERE id IN :ids"), {"ids": sub})
                            total_deleted += len(sub)
                            break
                        except Exception as e:
                            if "timeout" in str(e).lower():
                                time.sleep(2)
                                continue
                            raise e
        
        elapsed = time.time() - start_time
        pct = ((current_start - min_id) / (max_id - min_id)) * 100 if max_id > min_id else 100
        print(f"  Progress: {pct:.1f}% | Total Found: {total_found:,} | Total Removed: {total_deleted:,} | Time: {elapsed:.0f}s")
        sys.stdout.flush()
        
        current_start = current_end
        time.sleep(0.05)

    print("\n" + "=" * 70)
    print(f"SUCCESS! Total duplicates removed: {total_deleted:,}")
    print(f"Total time: {time.time() - start_time:.1f}s")
    print("=" * 70)

except Exception as e:
    print(f"FATAL ERROR: {e}")
    sys.exit(1)
