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

print("Deduplication started by City (using prefix index)...")

with engine.connect() as conn:
    print("Fetching city list...")
    cities = [r[0] for r in conn.execute(text("SELECT DISTINCT city FROM raw_google_map_drive_data")).fetchall()]

print(f"Total Unique Cities: {len(cities):,}")
sys.stdout.flush()

for i, city in enumerate(cities):
    try:
        # Some cities might be NULL
        city_val = city if city is not None else ""
        
        with engine.begin() as conn:
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 20"))
            
            # Delete duplicates WITHIN this city
            # This is fast because of idx_city (though it is a prefix index, it narrows down very well)
            sql = text("""
                DELETE t1 FROM raw_google_map_drive_data t1
                INNER JOIN raw_google_map_drive_data t2
                ON t1.id > t2.id
                AND COALESCE(t1.name,'') = COALESCE(t2.name,'')
                AND COALESCE(t1.phone_number,'') = COALESCE(t2.phone_number,'')
                AND COALESCE(t1.city,'') = COALESCE(t2.city,'')
                AND COALESCE(t1.address,'') = COALESCE(t2.address,'')
                WHERE (t1.city = :city OR (t1.city IS NULL AND :city = ''))
                AND (t2.city = :city OR (t2.city IS NULL AND :city = ''))
            """)
            
            result = conn.execute(sql, {"city": city_val})
            deleted = result.rowcount
            total_deleted += deleted
            
            if i % 10 == 0 or deleted > 0:
                elapsed = time.time() - start
                pct = (i+1)/len(cities) * 100
                print(f"  [{i+1}/{len(cities)}] City: {str(city)[:20]:<22} | Deleted: {deleted:>4} | Total: {total_deleted:,} | {pct:.1f}% | {elapsed:.0f}s")
                sys.stdout.flush()
                
    except Exception as e:
        print(f"  Error on city {city}: {str(e)[:100]}")
        sys.stdout.flush()

print("\n" + "=" * 70)
print(f"FINISHED! Removed {total_deleted:,} duplicates.")
print(f"Total Time: {time.time()-start:.1f}s")
with engine.connect() as conn:
    final_count = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).fetchone()[0]
    print(f"Final Row Count: {final_count:,}")
print("=" * 70)
