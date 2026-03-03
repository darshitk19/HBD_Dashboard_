import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
load_dotenv()
engine = create_engine(f'mysql+pymysql://{os.getenv("DB_USER")}:{quote_plus(os.getenv("DB_PASSWORD_PLAIN", ""))}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT", "3306")}/{os.getenv("DB_NAME")}')
with engine.connect() as conn:
    sql = text("""
        SELECT r.drive_file_id, COUNT(*) as actual_rows, MAX(reg.last_processed_row) as expected_rows
        FROM raw_google_map_drive_data r
        JOIN file_registry reg ON r.drive_file_id = reg.drive_file_id
        GROUP BY r.drive_file_id
        HAVING actual_rows > expected_rows
        LIMIT 20
    """)
    res = conn.execute(sql).fetchall()
    print("Files with more rows than expected:")
    for row in res:
        print(row)
