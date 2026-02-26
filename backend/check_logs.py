import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv(r'd:\Honeybee digital\Dashboard latest\backend\.env')
db_pass = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DATABASE_URI = f"mysql+pymysql://{os.getenv('DB_USER')}:{db_pass}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URI)

with engine.connect() as conn:
    print("Last 10 validation logs (with missing_count):")
    try:
        res = conn.execute(text("SELECT id, total_processed, missing_count, valid_count, duplicate_count, cleaned_count, last_id, timestamp FROM data_validation_log ORDER BY id DESC LIMIT 10")).fetchall()
        for row in res:
            print(row)
    except Exception as e:
        print(f"Validation log error: {e}")
