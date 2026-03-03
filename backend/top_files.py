import os
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = urllib.parse.quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)

with engine.connect() as conn:
    print("Top 20 Files by Record Count in Raw Table:")
    res = conn.execute(text("SELECT drive_file_name, COUNT(*) as cnt FROM raw_google_map_drive_data GROUP BY drive_file_name ORDER BY cnt DESC LIMIT 20")).fetchall()
    for r in res:
        print(f"  - {r[0]}: {r[1]:,} rows")
