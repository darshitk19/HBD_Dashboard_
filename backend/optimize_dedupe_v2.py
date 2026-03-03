
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

with engine.begin() as conn:
    print("Checking if signature_hash exists...")
    res = conn.execute(text("SHOW COLUMNS FROM raw_clean_google_map_data LIKE 'signature_hash'"))
    if not res.fetchone():
        print("Adding signature_hash column...")
        conn.execute(text("ALTER TABLE raw_clean_google_map_data ADD COLUMN signature_hash VARCHAR(64) AFTER raw_id"))
        print("Adding index...")
        conn.execute(text("CREATE INDEX idx_sig_hash ON raw_clean_google_map_data(signature_hash)"))
    else:
        print("Column already exists.")
