import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN','') or '')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}", pool_pre_ping=True)

with engine.connect() as conn:
    print("Fetching total vs unique counts...")
    total = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).fetchone()[0]
    
    # Using a nested query to count unique combinations
    unique_count = conn.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM raw_google_map_drive_data
            GROUP BY 
                COALESCE(name, ''), 
                COALESCE(address, ''), 
                COALESCE(phone_number, ''), 
                COALESCE(city, ''), 
                COALESCE(category, ''), 
                COALESCE(subcategory, '')
        ) as t
    """)).fetchone()[0]
    
    print(f"Total rows: {total:,}")
    print(f"Unique records: {unique_count:,}")
    print(f"Remaining duplicates: {total - unique_count:,}")
