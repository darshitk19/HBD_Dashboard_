import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
load_dotenv()
engine = create_engine(f'mysql+pymysql://{os.getenv("DB_USER")}:{quote_plus(os.getenv("DB_PASSWORD_PLAIN", ""))}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT", "3306")}/{os.getenv("DB_NAME")}')
with engine.connect() as conn:
    # Kill the IDs found earlier
    for qid in [686, 687, 698]:
        try:
            print(f"Killing process {qid}...")
            conn.execute(text(f"KILL {qid}"))
        except Exception as e:
            print(f"Could not kill {qid}: {e}")
