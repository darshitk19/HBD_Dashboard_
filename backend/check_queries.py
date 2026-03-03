import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
load_dotenv()
engine = create_engine(f'mysql+pymysql://{os.getenv("DB_USER")}:{quote_plus(os.getenv("DB_PASSWORD_PLAIN", ""))}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT", "3306")}/{os.getenv("DB_NAME")}')
with engine.connect() as conn:
    print("MySQL PROCESSLIST:")
    rows = conn.execute(text("SHOW FULL PROCESSLIST")).fetchall()
    for row in rows:
        # Filter for the long running query
        if "GROUP BY" in str(row[7]) or "DELETE" in str(row[7]):
            print(f"ID: {row[0]} | State: {row[6]} | Time: {row[5]} | Query: {str(row[7])[:150]}")
        else:
            print(f"ID: {row[0]} | State: {row[6]} | Time: {row[5]}")
