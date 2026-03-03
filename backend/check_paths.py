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
    print("Top 20 Paths in raw_google_map_drive_data:")
    res = conn.execute(text("SELECT full_drive_path, COUNT(*) as cnt FROM raw_google_map_drive_data GROUP BY full_drive_path ORDER BY cnt DESC LIMIT 20")).fetchall()
    for r in res:
        print(f"{r[0]}: {r[1]:,}")
