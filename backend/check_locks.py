import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN','') or '')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}")

with engine.connect() as conn:
    print("--- INNODB LOCKS / STATUS ---")
    try:
        # For MySQL 8+, use performance_schema or data_locks if available
        # But SHOW ENGINE INNODB STATUS is most reliable for deadlocks
        res = conn.execute(text("SHOW ENGINE INNODB STATUS")).fetchone()
        if res:
            print(res[2]) # The Status column
    except Exception as e:
        print(f"Error checking status: {e}")

    print("\n--- ACTIVE TRANSACTIONS ---")
    try:
        res = conn.execute(text("SELECT * FROM information_schema.innodb_trx")).fetchall()
        for r in res:
            print(r)
    except Exception as e:
        print(f"Error checking trx: {e}")
