import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)

with engine.connect() as conn:
    # 1. File Registry Status
    res = conn.execute(text("SELECT status, COUNT(*) FROM file_registry GROUP BY status")).fetchall()
    print("\nFile Registry Status:")
    for status, count in res:
        print(f"  - {status}: {count} files")
    
    # 2. Validation Log (Last 5 batches)
    print("\nRecent Activity (Last Batch):")
    log_res = conn.execute(text("SELECT total_processed, valid_count, missing_count, duplicate_count, cleaned_count FROM data_validation_log ORDER BY id DESC LIMIT 1")).fetchone()
    if log_res:
        print(f"  - Total Processed: {log_res[0]}")
        print(f"  - Valid: {log_res[1]}")
        print(f"  - Missing: {log_res[2]}")
        print(f"  - Duplicate: {log_res[3]}")
        print(f"  - Cleaned to Master: {log_res[4]}")

    # 3. Overall Record Counts
    raw = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).fetchone()[0]
    clean = conn.execute(text("SELECT COUNT(*) FROM raw_clean_google_map_data")).fetchone()[0]
    master = conn.execute(text("SELECT COUNT(*) FROM g_map_master_table")).fetchone()[0]
    print(f"\nOverall Counts:")
    print(f"  - Raw Records: {raw:,}")
    print(f"  - Clean Records: {clean:,}")
    print(f"  - Master Records: {master:,}")

    # Calculate pending records for validation/cleaning
    last_processed_id = 0
    res_id = conn.execute(text("SELECT meta_value FROM etl_metadata WHERE meta_key='last_processed_id'")).fetchone()
    if res_id:
        last_processed_id = int(res_id[0])
    
    pending_records = raw - clean
    print(f"  - Records awaiting Validation/Cleaning: {pending_records:,}")
