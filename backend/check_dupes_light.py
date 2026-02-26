"""Ultra-light duplicate check - uses only indexed columns."""
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

import urllib.parse
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = urllib.parse.quote_plus(os.getenv('DB_PASSWORD') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}", 
                       connect_args={'read_timeout': 300, 'write_timeout': 300})

with engine.connect() as conn:
    conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
    
    # Fast check: total rows vs unique (name, phone_number) combos
    # We use a sampling approach to avoid timeout
    print("Sampling 100,000 rows for duplicate analysis...")
    
    sample = conn.execute(text("""
        SELECT COUNT(*) as total, COUNT(DISTINCT CONCAT(COALESCE(name,''), '|', COALESCE(phone_number,''))) as unique_combos 
        FROM (SELECT name, phone_number FROM raw_google_map_drive_data LIMIT 100000) sub
    """)).fetchone()
    
    total_sample = sample[0]
    unique_sample = sample[1]
    dupe_pct = ((total_sample - unique_sample) / total_sample * 100) if total_sample else 0
    
    print(f"Sample: {total_sample:,} rows, {unique_sample:,} unique (name+phone)")
    print(f"Estimated duplicate rate: {dupe_pct:.1f}%")
    print(f"Estimated duplicates in full DB: ~{int(13469698 * dupe_pct / 100):,}")
