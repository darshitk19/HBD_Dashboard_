
import os
import sys
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
db_user = os.getenv('DB_USER')
db_pass = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or '')
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')
db_port = os.getenv('DB_PORT')

# Fix for Windows console encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

engine = create_engine(f'mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

with engine.connect() as conn:
    print("--- FAILED FILES IN REGISTRY ---")
    failed_files = conn.execute(text("SELECT drive_file_id, filename, status, error_message FROM file_registry WHERE status = 'ERROR' OR error_message IS NOT NULL LIMIT 10")).fetchall()
    for f in failed_files:
        print(f"File: {f[1]} | Status: {f[2]} | Error: {f[3]}")
    
    print("\n--- RECENT VALIDATION ERRORS ---")
    # Check if we have an error table or column
    try:
        val_errors = conn.execute(text("SELECT id, name, validation_status, missing_fields, invalid_format_fields FROM raw_clean_google_map_data WHERE validation_status IN ('INVALID', 'MISSING') ORDER BY id DESC LIMIT 5")).fetchall()
        for v in val_errors:
            print(f"ID: {v[0]} | Name: {v[1]} | Status: {v[2]} | Missing: {v[3]} | Invalid: {v[4]}")
    except Exception as e:
        print(f"Could not read clean table errors: {e}")

    print("\n--- DB THREADS ---")
    print('Threads connected:', conn.execute(text('SHOW STATUS LIKE "Threads_connected"')).fetchone())
