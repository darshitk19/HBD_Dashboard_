"""Quick duplicate check using lightweight queries."""
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = urllib.parse.quote_plus(os.getenv('DB_PASSWORD') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

with engine.connect() as conn:
    conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
    
    # 1. Total rows
    total = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).scalar()
    print(f"Total rows: {total:,}")
    
    # 2. Unique file_ids (indexed - fast)
    unique_files = conn.execute(text("SELECT COUNT(DISTINCT drive_file_id) FROM raw_google_map_drive_data")).scalar()
    print(f"Unique drive_file_ids: {unique_files:,}")
    
    # 3. Quick sample: Check if same file_id appears with multiple rows
    sample = conn.execute(text("""
        SELECT drive_file_id, drive_file_name, COUNT(*) as rows_from_file 
        FROM raw_google_map_drive_data 
        GROUP BY drive_file_id, drive_file_name 
        ORDER BY rows_from_file DESC 
        LIMIT 5
    """)).fetchall()
    print(f"\nTop 5 files by row count:")
    for r in sample:
        print(f"  {r[1]}: {r[2]:,} rows")

    # 4. Check for actual business-level duplicates using a LIMIT sample
    print(f"\nChecking for duplicate businesses (same name + phone)...")
    dupe_check = conn.execute(text("""
        SELECT name, phone_number, COUNT(*) as cnt 
        FROM raw_google_map_drive_data
        WHERE name IS NOT NULL AND phone_number IS NOT NULL AND phone_number != ''
        GROUP BY name, phone_number
        HAVING cnt > 1
        LIMIT 10
    """)).fetchall()
    
    if dupe_check:
        print(f"⚠️  Duplicate businesses found (same name + phone):")
        for r in dupe_check:
            print(f"  {r[0]} | Phone: {r[1]} | {r[2]} copies")
    else:
        print("✅ No duplicate businesses detected in sample.")
