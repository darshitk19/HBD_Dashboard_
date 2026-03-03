import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
load_dotenv()
engine = create_engine(f'mysql+pymysql://{os.getenv("DB_USER")}:{quote_plus(os.getenv("DB_PASSWORD_PLAIN", ""))}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT", "3306")}/{os.getenv("DB_NAME")}')
try:
    with engine.connect() as conn:
        print("Testing basic query...")
        res = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).fetchone()
        print(f"Row count: {res[0]}")
except Exception as e:
    print(f"Error: {e}")
