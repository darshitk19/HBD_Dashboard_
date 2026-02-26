import sys, os
sys.stdout = open(sys.stdout.fileno(), 'w', encoding='utf-8')
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or '')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

with engine.connect() as conn:
    # Total counts
    total = conn.execute(text("SELECT COUNT(*) FROM raw_clean_google_map_data")).fetchone()[0]
    dupes = conn.execute(text("SELECT COUNT(*) FROM raw_clean_google_map_data WHERE validation_status='DUPLICATE'")).fetchone()[0]
    valid = conn.execute(text("SELECT COUNT(*) FROM raw_clean_google_map_data WHERE validation_status='VALID'")).fetchone()[0]
    missing = conn.execute(text("SELECT COUNT(*) FROM raw_clean_google_map_data WHERE validation_status='MISSING'")).fetchone()[0]
    invalid = conn.execute(text("SELECT COUNT(*) FROM raw_clean_google_map_data WHERE validation_status='INVALID'")).fetchone()[0]
    
    print("=" * 70)
    print("  CLEAN TABLE SUMMARY")
    print("=" * 70)
    print(f"  Total Records : {total:,}")
    print(f"  VALID         : {valid:,}")
    print(f"  MISSING       : {missing:,}")
    print(f"  INVALID       : {invalid:,}")
    print(f"  DUPLICATE     : {dupes:,}")
    print("=" * 70)
    
    # Duplicate breakdown by state
    print("\n  DUPLICATES BY STATE (Top 15)")
    print("-" * 50)
    rows = conn.execute(text("""
        SELECT state, COUNT(*) as cnt 
        FROM raw_clean_google_map_data 
        WHERE validation_status='DUPLICATE' 
        GROUP BY state 
        ORDER BY cnt DESC 
        LIMIT 15
    """)).fetchall()
    for r in rows:
        print(f"  {r[0] or 'Unknown':<30} : {r[1]:,}")
    
    # Sample duplicates
    print(f"\n  SAMPLE DUPLICATES (Latest 25)")
    print("-" * 100)
    print(f"  {'ID':<8} {'RawID':<8} {'Name':<35} {'Phone':<15} {'City':<20} {'State':<20}")
    print("-" * 100)
    rows = conn.execute(text("""
        SELECT id, raw_id, name, phone_number, city, state 
        FROM raw_clean_google_map_data 
        WHERE validation_status='DUPLICATE' 
        ORDER BY id DESC 
        LIMIT 25
    """)).fetchall()
    for r in rows:
        name = (r[2] or 'N/A')[:33]
        phone = (r[3] or 'N/A')[:13]
        city = (r[4] or 'N/A')[:18]
        state = (r[5] or 'N/A')[:18]
        print(f"  {r[0]:<8} {r[1]:<8} {name:<35} {phone:<15} {city:<20} {state:<20}")
    print("=" * 100)
