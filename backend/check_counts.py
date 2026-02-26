import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

db_pass = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DATABASE_URI = f"mysql+pymysql://{os.getenv('DB_USER')}:{db_pass}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URI)

with engine.connect() as conn:
    res = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).fetchone()
    print(f"Total rows in raw_google_map_drive_data: {res[0]}")
    
    res = conn.execute(text("SELECT status, start_time FROM etl_scan_log ORDER BY id DESC LIMIT 5")).fetchall()
    print("\nRecent Scans:")
    for r in res:
        print(f"Status: {r[0]}, Start: {r[1]}")
