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

try:
    with engine.connect() as conn:
        print("Detailed Index List for raw_google_map_drive_data:")
        res = conn.execute(text("SHOW INDEX FROM raw_google_map_drive_data")).fetchall()
        for r in res:
            # Index name is at index 2 of the result row
            print(f"- {r[2]} (Column: {r[4]})")
            
except Exception as e:
    print(f"Error: {e}")
