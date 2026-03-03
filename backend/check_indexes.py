import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
load_dotenv()
engine = create_engine(f'mysql+pymysql://{os.getenv("DB_USER")}:{quote_plus(os.getenv("DB_PASSWORD_PLAIN", ""))}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT", "3306")}/{os.getenv("DB_NAME")}')
with engine.connect() as conn:
    print("INDEXES ON raw_google_map_drive_data:")
    for row in conn.execute(text('SHOW INDEX FROM raw_google_map_drive_data')).fetchall():
        print(row)
