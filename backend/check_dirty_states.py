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

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

try:
    with engine.connect() as conn:
        print("--- State Distribution ---")
        res = conn.execute(text("SELECT state, COUNT(*) FROM raw_google_map_drive_data GROUP BY state LIMIT 50"))
        for row in res:
            print(f"[{repr(row[0])}] : {row[1]} rows")
            
except Exception as e:
    print(f"Error: {e}")
