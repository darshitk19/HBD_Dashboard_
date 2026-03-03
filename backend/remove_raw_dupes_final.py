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
seen = set()
to_delete = []

with engine.connect() as conn:
    max_id = conn.execute(text("SELECT MAX(id) FROM raw_google_map_drive_data")).fetchone()[0]
    min_id = conn.execute(text("SELECT MIN(id) FROM raw_google_map_drive_data")).fetchone()[0]

print(f"ID Range: {min_id} to {max_id}. Total to scan: {max_id - min_id:,}")
sys.stdout.flush()

CHUNK_SIZE = 50000
current_id = min_id

print("Step 1: Identifying Duplicates (using ID chunks)...")
while current_id <= max_id:
    try:
        with engine.connect() as conn:
            # Explicitly fetch a small range of IDs to avoid large memory/temp disk usage
            rows = conn.execute(text("""
                SELECT id, name, phone_number, city, address 
                FROM raw_google_map_drive_data 
                WHERE id >= :start AND id < :end
            """), {"start": current_id, "end": current_id + CHUNK_SIZE}).fetchall()
            
            for row in rows:
                # Signature: (name, phone, city, address) normalized
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
            
            current_id += CHUNK_SIZE
            if (current_id - min_id) % (CHUNK_SIZE * 5) == 0:
                print(f"  Scanned up to ID {current_id:,}... Found {len(to_delete):,} dupes in Python memory")
                sys.stdout.flush()
                
    except Exception as e:
        print(f"Error at ID {current_id}: {e}")
        time.sleep(1)

print(f"Step 1 Complete. Found {len(to_delete):,} duplicates.")
sys.stdout.flush()

if not to_delete:
    print("No duplicates found. Finished.")
    sys.exit(0)

# Step 2: Delete in batches
print(f"\nStep 2: Deleting {len(to_delete):,} duplicates...")
sys.stdout.flush()

DELETE_BATCH = 5000
deleted_count = 0
for i in range(0, len(to_delete), DELETE_BATCH):
    batch = to_delete[i:i + DELETE_BATCH]
    try:
        with engine.begin() as conn:
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 10"))
            conn.execute(text("DELETE FROM raw_google_map_drive_data WHERE id IN :ids"), {"ids": batch})
        deleted_count += len(batch)
        if deleted_count % (DELETE_BATCH * 10) == 0:
            print(f"  Deleted: {deleted_count:,}/{len(to_delete):,}")
            sys.stdout.flush()
    except Exception as e:
        print(f"Error in delete batch: {e}")
        time.sleep(1)

print("\n" + "=" * 70)
print(f"SUCCESS! Removed {deleted_count:,} duplicates.")
print(f"Time: {time.time() - start_time:.1f}s")
print("=" * 70)
