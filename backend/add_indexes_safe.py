import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = quote_plus(os.getenv('DB_PASSWORD') or "") 
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def run_sql(sql):
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
            print(f"Success: {sql}")
    except Exception as e:
        print(f"Skipped/Error: {sql} | {str(e)[:100]}")

# Use the correct table and column names
run_sql("CREATE INDEX IF NOT EXISTS idx_drive_file_id ON raw_google_map_drive_data (drive_file_id)")
run_sql("CREATE INDEX IF NOT EXISTS idx_business_grouping ON raw_google_map_drive_data (name(50), address(100))")
print("Done.")
