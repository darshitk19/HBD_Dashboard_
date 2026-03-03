import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN','') or '')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}")

with engine.connect() as conn:
    print("--- PROCESSLIST ---")
    res = conn.execute(text("SHOW PROCESSLIST")).fetchall()
    for r in res:
        print(r)
    
    print("\n--- IN_PROGRESS COUNT ---")
    cnt = conn.execute(text("SELECT COUNT(*) FROM file_registry WHERE status='IN_PROGRESS'")).fetchone()[0]
    print(f"Total IN_PROGRESS: {cnt}")
