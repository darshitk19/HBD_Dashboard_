import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv(r'd:\Honeybee digital\Dashboard latest\backend\.env')
db_pass = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DATABASE_URI = f"mysql+pymysql://{os.getenv('DB_USER')}:{db_pass}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URI)

from model.robust_gdrive_etl_v2 import ValidationQualityProcessor
import threading

shutdown_event = threading.Event()
validator = ValidationQualityProcessor(engine, shutdown_event)

with engine.connect() as conn:
    res = conn.execute(text("SELECT id, name, address, website, phone_number, city, state, category FROM raw_google_map_drive_data WHERE id > 312265 ORDER BY id ASC LIMIT 10")).fetchall()
    for row in res:
        row_dict = dict(row._mapping)
        is_structured, is_valid, missing, invalid, clean_phone = validator.validate_row(row_dict)
        print(f"ID: {row_dict['id']} | Structured: {is_structured} | Valid: {is_valid} | Missing: {missing} | Invalid: {invalid} | Phone: {row_dict['phone_number']}")
