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
    print("Checking near-duplicates for Tumbledry (7677250250):")
    res = conn.execute(text("SELECT id, reviews_count, reviews_average, category, etl_version, LEFT(address, 30) FROM raw_google_map_drive_data WHERE phone_number = '7677250250'")).fetchall()
    for r in res:
        print(f"  ID: {r[0]}, Reviews: {r[1]}, Avg: {r[2]}, Cat: {r[3]}, Ver: {r[4]}, Addr: {r[5]}")
