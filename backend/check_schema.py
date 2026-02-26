import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv(r'd:\Honeybee digital\Dashboard latest\backend\.env')
db_pass = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DATABASE_URI = f"mysql+pymysql://{os.getenv('DB_USER')}:{db_pass}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URI)

with engine.connect() as conn:
    print("raw_clean_google_map_data indexes:")
    try:
        res = conn.execute(text("SHOW INDEX FROM raw_clean_google_map_data")).fetchall()
        for r in res:
            print(r)
    except Exception as e:
        print(f"Error: {e}")
