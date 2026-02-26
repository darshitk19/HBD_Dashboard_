import os
import time
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

try:
    with engine.connect() as conn:
        print("Timing SELECT DISTINCT state...")
        start = time.time()
        res = conn.execute(text("SELECT DISTINCT state FROM raw_google_map_drive_data WHERE state IS NOT NULL AND state != ''")).fetchall()
        print(f"DONE. Took {time.time()-start:.2f}s")
        print(f"States found: {len(res)}")
        for r in res[:10]:
            print(f" - {r[0]}")
            
except Exception as e:
    print(f"Error: {e}")
