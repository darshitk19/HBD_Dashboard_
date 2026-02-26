import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv(r'd:\Honeybee digital\Dashboard latest\backend\.env')
db_pass = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DATABASE_URI = f"mysql+pymysql://{os.getenv('DB_USER')}:{db_pass}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URI)

with engine.connect() as conn:
    try:
        log = conn.execute(text("SELECT id, last_id FROM data_validation_log ORDER BY id DESC LIMIT 1")).fetchone()
        print("Last log:", log)
    except Exception as e: print("Log error:", e)

    try:
        raw_max = conn.execute(text("SELECT MAX(id) FROM raw_google_map_drive_data")).fetchone()[0]
        print("Max raw ID:", raw_max)
    except Exception as e: print("Raw max error:", e)
