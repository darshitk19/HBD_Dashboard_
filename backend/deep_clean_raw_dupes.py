import os
import sys
import time
import hashlib
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

# Database setup
DB_USER = os.getenv('DB_USER')
DB_PASS = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI, pool_pre_ping=True)

def get_row_signature(row):
    """Matches the logic in etl_tasks.py exactly."""
    parts = [
        str(row['name'] or "").lower().strip(),
        str(row['address'] or "").lower().strip(),
        str(row['website'] or "").lower().strip(),
        str(row['phone_number'] or "").lower().strip(),
        str(row['reviews_count'] or 0),
        str(row['reviews_average'] or 0.0),
        str(row['category'] or "").lower().strip(),
        str(row['subcategory'] or "").lower().strip(),
        str(row['city'] or "").lower().strip(),
        str(row['state'] or "").lower().strip(),
        str(row['area'] or "").lower().strip()
    ]
    sig_str = "|".join(parts)
    return hashlib.md5(sig_str.encode('utf-8', errors='ignore')).hexdigest()

def main():
    start_time = time.time()
    print("Starting Deep Cleanup: Removing exact duplicates based on 11 business columns.")
    
    seen_signatures = set()
    to_delete = []
    total_processed = 0
    
    # We fetch rows in chunks to avoid memory issues
    CHUNK_SIZE = 100000
    
    try:
        with engine.connect() as conn:
            # 1. Get total row count for progress
            total_rows = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).scalar()
            print(f"Total rows to analyze: {total_rows:,}")
            
            # 2. Iterate through all rows
            # We use a simple select with ID order to be consistent
            # Note: Fetching 4M rows with 11 columns will take some time
            query = text("""
                SELECT id, name, address, website, phone_number, 
                       reviews_count, reviews_average, 
                       category, subcategory, city, state, area 
                FROM raw_google_map_drive_data 
                ORDER BY id ASC
            """)
            
            result_proxy = conn.execution_options(stream_results=True).execute(query)
            
            print("Status: Scanning for duplicates...")
            
            for row in result_proxy:
                total_processed += 1
                
                # Convert row to dictionary for signature helper
                sig = get_row_signature({
                    'name': row[1], 'address': row[2], 'website': row[3],
                    'phone_number': row[4], 'reviews_count': row[5], 'reviews_average': row[6],
                    'category': row[7], 'subcategory': row[8], 'city': row[9],
                    'state': row[10], 'area': row[11]
                })
                
                if sig in seen_signatures:
                    to_delete.append(row[0]) # Add ID to delete list
                else:
                    seen_signatures.add(sig)
                
                if total_processed % 100000 == 0:
                    print(f"  Processed {total_processed:,}/{total_rows:,} | Found {len(to_delete):,} duplicates")
                    sys.stdout.flush()

        print(f"\nScan complete. Total duplicates found: {len(to_delete):,}")
        
        if not to_delete:
            print("No exact duplicates found. Table is already clean.")
            return

        # 3. Batch Deletion
        DELETE_BATCH = 5000
        removed_count = 0
        print(f"Status: Deleting {len(to_delete):,} rows in batches of {DELETE_BATCH}...")
        
        for i in range(0, len(to_delete), DELETE_BATCH):
            batch = to_delete[i:i+DELETE_BATCH]
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM raw_google_map_drive_data WHERE id IN :ids"), {"ids": batch})
            removed_count += len(batch)
            if removed_count % 50000 == 0:
                print(f"  Deleted {removed_count:,}/{len(to_delete):,}...")
                sys.stdout.flush()
            # Minor sleep to prevent DB lock contention
            time.sleep(0.01)

        print(f"\nSUCCESS! Removed {removed_count:,} exact duplicates in {time.time() - start_time:.1f}s")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
