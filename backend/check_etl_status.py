import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN','') or '')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}")

with engine.connect() as conn:
    print("ETL FILE STATUS SUMMARY:")
    print("-" * 30)
    
    # 1. Check folders status
    folder_stats = conn.execute(text("SELECT status, COUNT(*) FROM drive_folder_registry GROUP BY status")).fetchall()
    print("Folders:")
    for status, count in folder_stats:
        print(f"  {status}: {count}")
    
    # 2. Check files status in registry
    file_stats = conn.execute(text("SELECT status, COUNT(*) FROM file_registry GROUP BY status")).fetchall()
    print("\nFiles in Registry:")
    for status, count in file_stats:
        print(f"  {status}: {count}")
    
    # 3. Check for any files stuck in IN_PROGRESS or ERROR
    stuck_files = conn.execute(text("SELECT filename, status, error_message FROM file_registry WHERE status NOT IN ('PROCESSED', 'DONE') LIMIT 10")).fetchall()
    if stuck_files:
        print("\nRecently Failed/In-Progress Examples:")
        for name, status, err in stuck_files:
            print(f"  [{status}] {name}: {str(err)[:50]}...")
    
    # 4. Check total unique files in raw table vs registry
    raw_files = conn.execute(text("SELECT COUNT(DISTINCT drive_file_id) FROM raw_google_map_drive_data")).fetchone()[0]
    reg_files = conn.execute(text("SELECT COUNT(*) FROM file_registry WHERE status='PROCESSED'")).fetchone()[0]
    print(f"\nUnique Files with data in Raw: {raw_files:,}")
    print(f"Files marked PROCESSED in Registry: {reg_files:,}")
