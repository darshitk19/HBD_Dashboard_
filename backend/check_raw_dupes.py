import sys, os
sys.stdout = open(sys.stdout.fileno(), 'w', encoding='utf-8')
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD_PLAIN',''))}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','3306')}/{os.getenv('DB_NAME')}",
    pool_pre_ping=True
)

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT name, phone_number, city, address, COUNT(*) as cnt
        FROM raw_google_map_drive_data
        GROUP BY name, phone_number, city, address
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 20
    """)).fetchall()
    
    print(f"{'Name':<40} {'Phone':<15} {'City':<20} {'Copies':<6}")
    print("-" * 85)
    for r in rows:
        name = (r[0] or 'N/A')[:38]
        phone = (r[1] or 'N/A')[:13]
        city = (r[2] or 'N/A')[:18]
        print(f"{name:<40} {phone:<15} {city:<20} {r[4]}")
