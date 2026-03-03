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
print("Deduplication Status: Starting Incremental High-Speed Index Scan...")
sys.stdout.flush()

try:
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS _dupe_scan"))
        conn.execute(text("""
            CREATE TABLE _dupe_scan (
                id BIGINT PRIMARY KEY,
                name VARCHAR(100),
                phone VARCHAR(50),
                city VARCHAR(50),
                address VARCHAR(100)
            ) ENGINE=InnoDB
        """))

    with engine.connect() as conn:
        max_id = conn.execute(text("SELECT MAX(id) FROM raw_google_map_drive_data")).fetchone()[0]
        min_id = conn.execute(text("SELECT MIN(id) FROM raw_google_map_drive_data")).fetchone()[0]

    # Populating lookup table in chunks to keep the socket alive and show progress
    CHUNK = 200000
    current = min_id
    print(f"  Populating lookup table (IDs {min_id:,} to {max_id:,})...")
    while current <= max_id:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO _dupe_scan (id, name, phone, city, address)
                SELECT id, 
                       LEFT(COALESCE(name, ''), 100), 
                       LEFT(COALESCE(phone_number, ''), 50), 
                       LEFT(COALESCE(city, ''), 50), 
                       LEFT(COALESCE(address, ''), 100)
                FROM raw_google_map_drive_data
                WHERE id >= :start AND id < :end
            """), {"start": current, "end": current + CHUNK})
        current += CHUNK
        print(f"    Scanned up to ID {current:,}...")
        sys.stdout.flush()

    print("  Adding composite index...")
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX idx_lookup ON _dupe_scan(name, phone, city, address)"))
    print(f"Lookup table ready in {time.time() - start_time:.1f}s")
    sys.stdout.flush()

    # Find duplicates
    print("  Identifying IDs to delete...")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS _ids_to_del"))
        conn.execute(text("""
            CREATE TABLE _ids_to_del (id BIGINT PRIMARY KEY)
            SELECT t1.id
            FROM _dupe_scan t1
            INNER JOIN (
                SELECT MIN(id) as keep_id, name, phone, city, address
                FROM _dupe_scan
                GROUP BY name, phone, city, address
                HAVING COUNT(*) > 1
            ) t2 ON t1.name = t2.name 
            AND t1.phone = t2.phone 
            AND t1.city = t2.city 
            AND t1.address = t2.address
            WHERE t1.id > t2.keep_id
        """))

    with engine.connect() as conn:
        to_delete = conn.execute(text("SELECT COUNT(*) FROM _ids_to_del")).fetchone()[0]
    
    print(f"Total duplicates found: {to_delete:,}")
    sys.stdout.flush()

    # Batch delete
    current_removed = 0
    BATCH_SIZE = 10000
    while True:
        with engine.begin() as conn:
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 10"))
            # Delete from raw
            result = conn.execute(text("""
                DELETE r FROM raw_google_map_drive_data r
                INNER JOIN _ids_to_del d ON r.id = d.id
                LIMIT :batch
            """), {"batch": BATCH_SIZE})
            
            # Remove from to_delete list
            conn.execute(text("""
                DELETE FROM _ids_to_del WHERE id IN (
                    SELECT id FROM (SELECT id FROM _ids_to_del LIMIT :batch) tmp
                )
            """), {"batch": BATCH_SIZE})
            
            removed = result.rowcount
            current_removed += removed
            
            if removed == 0:
                break
            
            if current_removed % (BATCH_SIZE * 5) == 0:
                pct = (current_removed / to_delete) * 100 if to_delete > 0 else 100
                print(f"  Removed: {current_removed:,}/{to_delete:,} | {pct:.1f}%")
                sys.stdout.flush()
            
            time.sleep(0.05)

    # Cleanup
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS _dupe_scan"))
        conn.execute(text("DROP TABLE IF EXISTS _ids_to_del"))

    print("\n" + "=" * 70)
    print(f"SUCCESS! Removed {current_removed:,} duplicates.")
    print(f"Final Execution Time: {time.time() - start_time:.1f}s")
    print("=" * 70)

except Exception as e:
    print(f"FATAL ERROR: {e}")
    sys.exit(1)
