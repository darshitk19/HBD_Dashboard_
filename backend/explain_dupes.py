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
    print("Finding examples of Name+Phone+City duplicates in Raw that are NOT EXACT duplicates:")
    sql = text("""
        SELECT name, phone_number, city, COUNT(*) as cnt
        FROM raw_google_map_drive_data
        GROUP BY name, phone_number, city
        HAVING cnt > 1
        LIMIT 5
    """)
    dupes = conn.execute(sql).fetchall()
    
    for name, phone, city, cnt in dupes:
        print(f"\nBusiness: {name} | Phone: {phone} | City: {city} | Raw Count: {cnt}")
        # Look at the records to see what's different
        sql_rows = text("""
            SELECT id, reviews_count, reviews_average, LEFT(address, 50) as addr, category
            FROM raw_google_map_drive_data
            WHERE name = :n AND phone_number = :p AND city = :c
        """)
        rows = conn.execute(sql_rows, {"n": name, "p": phone, "c": city}).fetchall()
        for r in rows:
            print(f"  - ID: {r[0]}, Reviews: {r[1]}, Avg: {r[2]}, Cat: {r[4]}, Addr: {r[3]}")
