import sys, os, time, threading
sys.stdout = open(sys.stdout.fileno(), 'w', encoding='utf-8')
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN',''))}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','3306')}/{os.getenv('DB_NAME')}",
    pool_pre_ping=True
)

# Heartbeat thread to keep the process alive in the environment
def heartbeat():
    while True:
        print("  [HEARTBEAT] Still working...")
        sys.stdout.flush()
        time.sleep(15)

h = threading.Thread(target=heartbeat, daemon=True)
h.start()

start_time = time.time()
print("Starting Deduplication Process...")

try:
    with engine.connect() as conn:
        print("Finding IDs to keep...")
        # This GROUP BY is the heavy part. 
        # But with 16GB free disk, MySQL should be able to handle it now.
        sql = text("""
            SELECT MIN(id) 
            FROM raw_google_map_drive_data 
            GROUP BY COALESCE(name,''), COALESCE(phone_number,''), COALESCE(city,''), COALESCE(address,'')
        """)
        # We fetch ALL "keep IDs" into Python
        keep_ids = set(r[0] for r in conn.execute(sql))
        
        print(f"Found {len(keep_ids):,} unique records to keep.")
        
        print("Fetching all IDs from raw table...")
        all_ids = [r[0] for r in conn.execute(text("SELECT id FROM raw_google_map_drive_data"))]
        
        to_delete = [idx for idx in all_ids if idx not in keep_ids]
        print(f"Total duplicates to remove: {len(to_delete):,}")
        sys.stdout.flush()

    if not to_delete:
        print("No duplicates found.")
        sys.exit(0)

    # Delete in batches
    BATCH = 5000
    total_removed = 0
    for i in range(0, len(to_delete), BATCH):
        batch = to_delete[i:i+BATCH]
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM raw_google_map_drive_data WHERE id IN :ids"), {"ids": batch})
        total_removed += len(batch)
        if total_removed % 25000 == 0:
            print(f"  Removed {total_removed:,}/{len(to_delete):,}")
            sys.stdout.flush()
        time.sleep(0.05)

    print(f"DONE! Removed {total_removed:,} duplicates in {time.time()-start_time:.1f}s")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
