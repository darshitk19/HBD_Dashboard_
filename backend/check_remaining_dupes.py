import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN','') or '')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}")

with engine.connect() as conn:
    print("Checking for remaining duplicates based on 6 identity fields...")
    # This query finds groups where count > 1
    sql = text("""
        SELECT SUM(cnt - 1) as duplicate_count
        FROM (
            SELECT COUNT(*) as cnt
            FROM raw_google_map_drive_data
            GROUP BY 
                COALESCE(name, ''), 
                COALESCE(address, ''), 
                COALESCE(phone_number, ''), 
                COALESCE(city, ''), 
                COALESCE(category, ''), 
                COALESCE(subcategory, '')
            HAVING cnt > 1
        ) as dupe_groups
    """)
    res = conn.execute(sql).fetchone()
    print(f"Total remaining duplicate rows: {res[0] if res[0] else 0}")
