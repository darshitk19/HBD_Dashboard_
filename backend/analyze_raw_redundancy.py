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
    print("--- RAW DATA ANALYSIS ---")
    total = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).scalar()
    print(f"Total Rows in Raw: {total:,}")
    
    print("\nCounting Unique Identities (Name + Phone + City)...")
    unique_identities = conn.execute(text("SELECT COUNT(*) FROM (SELECT 1 FROM raw_google_map_drive_data GROUP BY name, phone_number, city) t")).scalar()
    print(f"Unique Identities: {unique_identities:,}")
    
    redundant = total - unique_identities
    print(f"Redundant/Near-Duplicate Rows: {redundant:,}")
    print(f"Identity Duplication Rate: {(redundant/total)*100:.2f}%")

    print("\n--- TOP 10 DUPLICATED IDENTITIES ---")
    top_dupes = conn.execute(text("""
        SELECT name, phone_number, city, COUNT(*) as cnt 
        FROM raw_google_map_drive_data 
        GROUP BY name, phone_number, city 
        ORDER BY cnt DESC 
        LIMIT 10
    """)).fetchall()
    
    for name, phone, city, cnt in top_dupes:
        print(f"  - {name} ({phone}) in {city}: {cnt} times")
