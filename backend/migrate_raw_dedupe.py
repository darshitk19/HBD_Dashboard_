import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RawDedupeMigrator")

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')
DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URI)

def migrate():
    with engine.connect() as conn:
        # 1. Add column if it doesn't exist
        try:
            logger.info("Adding row_signature column to raw_google_map_drive_data...")
            conn.execute(text("ALTER TABLE raw_google_map_drive_data ADD COLUMN row_signature VARCHAR(32) NULL AFTER file_hash"))
            conn.commit()
            logger.info("Column added successfully.")
        except Exception as e:
            if 'Duplicate column name' in str(e):
                logger.info("Column row_signature already exists.")
            else:
                logger.error(f"Error adding column: {e}")
                return

        # 2. Add Index. 
        # Note: We use a non-unique index first because existing rows have NULL row_signature.
        # Once workers start filling it, we could make it unique. 
        # BUT to stop duplicates NOW, we need IGNORE behavior.
        # Actually, MySQL handles UNIQUE indexes with multiple NULL values! 
        # (Each NULL is considered distinct from every other NULL).
        try:
            logger.info("Adding UNIQUE INDEX to row_signature...")
            # We use a unique index. This will allow multiple NULLs but block duplicate non-NULL hashes.
            conn.execute(text("ALTER TABLE raw_google_map_drive_data ADD UNIQUE INDEX idx_raw_signature (row_signature)"))
            conn.commit()
            logger.info("UNIQUE INDEX added successfully.")
        except Exception as e:
            if 'Duplicate key name' in str(e):
                logger.info("Index idx_raw_signature already exists.")
            else:
                logger.error(f"Error adding index: {e}")

if __name__ == "__main__":
    migrate()
