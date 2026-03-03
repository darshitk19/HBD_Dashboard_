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
    raw = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).scalar()
    clean = conn.execute(text("SELECT COUNT(*) FROM raw_clean_google_map_data")).scalar()
    master = conn.execute(text("SELECT COUNT(*) FROM g_map_master_table")).scalar()
    print(f"Raw: {raw:,}")
    print(f"Clean: {clean:,}")
    print(f"Master: {master:,}")
