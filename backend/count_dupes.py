
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
    print("Checking for duplicates in raw_google_map_drive_data...")
    # Signature: name, phone_number, city, address
    # Since address is text, we use a prefix or just count.
    # To be fast, let's just check (name, phone_number, city) first.
    res = conn.execute(text("""
        SELECT count(*) FROM (
            SELECT name, phone_number, city, address, count(*) 
            FROM raw_google_map_drive_data 
            GROUP BY name, phone_number, city, address(255)
            HAVING count(*) > 1
        ) as t
    """))
    print(f"Number of duplicate signatures: {res.fetchone()[0]}")
