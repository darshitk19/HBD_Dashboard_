import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
import time

load_dotenv()
engine = create_engine(f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN','') or '')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}")

with engine.connect() as conn:
    print("Checking IN_PROGRESS files progression...")
    res1 = conn.execute(text("SELECT drive_file_id, filename, last_processed_row FROM file_registry WHERE status='IN_PROGRESS'")).fetchall()
    
    print(f"Captured {len(res1)} files. Waiting 5s...")
    time.sleep(5)
    
    res2 = conn.execute(text("SELECT drive_file_id, filename, last_processed_row FROM file_registry WHERE status='IN_PROGRESS'")).fetchall()
    
    mapping = {r[0]: r[2] for r in res1}
    moved = 0
    for r in res2:
        old = mapping.get(r[0])
        if old is not None and r[2] > old:
            print(f"  [MOVING] {r[1]}: {old} -> {r[2]}")
            moved += 1
        elif old is not None:
            # print(f"  [STUCK] {r[1]}: {r[2]}")
            pass

    if moved == 0:
        print("ALERT: No files moved in 5 seconds. Workers might be stuck.")
    else:
        print(f"SUCCESS: {moved} files are actively processing.")
