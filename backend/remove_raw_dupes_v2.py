import sys, os, time
sys.stdout = open(sys.stdout.fileno(), 'w', encoding='utf-8')
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN',''))}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','3306')}/{os.getenv('DB_NAME')}",
    pool_pre_ping=True, pool_size=2, max_overflow=5
)

start = time.time()
total_deleted = 0

print("Deduplication started by File ID...")

with engine.connect() as conn:
    # Get all file IDs that have more than 0 rows
    print("Fetching file list...")
    file_ids = [r[0] for r in conn.execute(text("SELECT DISTINCT drive_file_id FROM raw_google_map_drive_data")).fetchall()]

print(f"Total Unique Files in Raw: {len(file_ids):,}")
sys.stdout.flush()

for i, fid in enumerate(file_ids):
    try:
        with engine.begin() as conn:
            # Set a timeout per file
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 30"))
            
            # Delete duplicates WITHIN this file only. 
            # Note: This only catches duplicates of the same business in the SAME file.
            # But the user said duplicates happened because files were re-added.
            # So if file A was added twice, it will have the same drive_file_id.
            # This logic will remove the second set of rows.
            
            sql = text("""
                DELETE t1 FROM raw_google_map_drive_data t1
                INNER JOIN raw_google_map_drive_data t2
                ON t1.drive_file_id = t2.drive_file_id
                AND t1.id > t2.id
                AND COALESCE(t1.name,'') = COALESCE(t2.name,'')
                AND COALESCE(t1.phone_number,'') = COALESCE(t2.phone_number,'')
                AND COALESCE(t1.city,'') = COALESCE(t2.city,'')
                AND COALESCE(t1.address,'') = COALESCE(t2.address,'')
                WHERE t1.drive_file_id = :fid
            """)
            
            result = conn.execute(sql, {"fid": fid})
            deleted = result.rowcount
            total_deleted += deleted
            
            if i % 100 == 0 or deleted > 0:
                elapsed = time.time() - start
                pct = (i+1)/len(file_ids) * 100
                print(f"  [{i+1}/{len(file_ids)}] File: {fid[:15]}... | Deleted: {deleted} | Total: {total_deleted:,} | {pct:.1f}% | {elapsed:.0f}s")
                sys.stdout.flush()
                
    except Exception as e:
        print(f"  Error on file {fid}: {str(e)[:100]}")
        sys.stdout.flush()

print("\n" + "=" * 70)
print(f"FINISHED! Removed {total_deleted:,} duplicates.")
print(f"Total Time: {time.time()-start:.1f}s")
with engine.connect() as conn:
    final_count = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).fetchone()[0]
    print(f"Final Row Count: {final_count:,}")
print("=" * 70)
