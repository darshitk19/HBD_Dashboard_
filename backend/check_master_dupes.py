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
    total = conn.execute(text("SELECT COUNT(*) FROM g_map_master_table")).fetchone()[0]
    unique = conn.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM g_map_master_table 
            GROUP BY COALESCE(name,''), COALESCE(phone_number,''), COALESCE(city,''), COALESCE(address,'')
        ) t
    """)).fetchone()[0]
    
    dupes = total - unique
    
    print(f"Total rows in master  : {total:,}")
    print(f"Unique rows           : {unique:,}")
    print(f"EXACT DUPLICATES      : {dupes:,}")
