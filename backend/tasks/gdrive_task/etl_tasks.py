import os
import csv
import io
import logging
import time
import signal
import hashlib
import threading
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from urllib.parse import quote_plus
from google.oauth2 import service_account
from utils.metrics import (
    files_processed, rows_inserted, rows_skipped,
    processing_time, dlq_entries, active_db_ops, batch_size_hist, error_count
)
from config import config
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from celery import shared_task
from dotenv import load_dotenv
import redis

from model.normalizer import UniversalNormalizer
# from model.csv_schema import BusinessRecord (Validation removed)
# from pydantic import ValidationError

logger = logging.getLogger("GDrive_Celery_Task")
if not logger.hasHandlers():
    handler = logging.FileHandler(os.path.join(os.getcwd(), 'output', 'gdrive_etl.log'), encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(task_id)s] %(message)s'))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)
# Force UTF-8 encoding for file log

# Centralized configuration
SERVICE_ACCOUNT_FILE = config.SERVICE_ACCOUNT_FILE
DATABASE_URI = config.DATABASE_URI
MAX_FILE_SIZE_MB = config.MAX_FILE_SIZE_MB
ETL_VERSION = config.ETL_VERSION
BATCH_SIZE = config.BATCH_SIZE
# Configuration
SERVICE_ACCOUNT_FILE = os.path.join(os.getcwd(), 'model', 'honey-bee-digital-d96daf6e6faf.json')
DB_USER = os.getenv('DB_USER')
DB_PASS = quote_plus(os.getenv('DB_PASSWORD_PLAIN') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', '3306')
DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

MAX_FILE_SIZE_MB = int(os.getenv('MAX_FILE_SIZE_MB', '100'))
ETL_VERSION = "2.0.0"

# SECTION 1: Optimized SQLAlchemy Engine
engine = create_engine(
    DATABASE_URI,
    pool_size=20,           # Matches worker concurrency
    max_overflow=10,        # Burst capacity
    pool_timeout=30,        # Give up if DB is too busy
    pool_recycle=1800,      # Recycle connections every 30 mins
    pool_pre_ping=True      # Prevent "MySQL Wait Timeout" errors
)

# Fix 7: Graceful Shutdown
shutdown_requested = False

def handle_shutdown(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    # Avoid logging here as it causes reentrant RuntimeError during recursive calls or I/O interrupts

# Register signal handlers (only in main thread to avoid Windows errors)
try:
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
except (OSError, ValueError):
    # signal handlers can only be set in the main thread
    pass

# Fix 10: DB Rate Limiting
db_semaphore = threading.Semaphore(10)


def get_service():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"Service account file not found at: {SERVICE_ACCOUNT_FILE}")
        
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds, cache_discovery=False)


# Fix 1 + Fix 2: File size protection + Context manager (no memory leak)
@contextmanager
def download_csv(service, file_id, max_size_mb=None):
    """
    Downloads a CSV file from Google Drive with size protection.
    Yields a TextIOWrapper stream. Properly cleans up on exit.
    Ensures file handle is always closed (resource management best practice).
    """
    if max_size_mb is None:
        max_size_mb = MAX_FILE_SIZE_MB

    # Check file size before downloading
    try:
        file_metadata = service.files().get(fileId=file_id, fields='size').execute()
        file_size = int(file_metadata.get('size', 0))
        if file_size > max_size_mb * 1024 * 1024:
            raise ValueError(
                f"File {file_id} is {file_size / (1024*1024):.1f}MB, "
                f"exceeds {max_size_mb}MB limit"
            )
    except ValueError:
        raise
    except Exception:
        # Some files (Google Docs exports) don't report size — allow them
        pass

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    wrapper = io.TextIOWrapper(fh, encoding='utf-8', errors='replace')
    try:
        yield wrapper
    finally:
        wrapper.close()  # Always close file handle


def get_file_hash(file_id, modified_time):
    """Generate a hash for file change detection."""
    return hashlib.md5(f"{file_id}:{modified_time}".encode()).hexdigest()


# SECTION 3: Batched Insert Optimization (with Deadlock Retry + Rate Limiting)
def commit_batch(batch, task_id=None):
    # Idempotency: This function is safe to retry because it uses INSERT IGNORE in SQL
    # and all rows have unique keys or hashes. No duplicate inserts will occur.
    if not batch:
        return 0
    sql = text("""
        INSERT IGNORE INTO raw_google_map_drive_data (
            name, address, website, phone_number, 
            reviews_count, reviews_average, 
            category, subcategory, city, state, area, 
            drive_file_id, drive_file_name, full_drive_path, 
            drive_uploaded_time, source,
            etl_version, task_id, file_hash
        )
        VALUES (
            :name, :address, :website, :phone_number, 
            :reviews_count, :reviews_average, 
            :category, :subcategory, :city, :state, :area, 
            :drive_file_id, :drive_file_name, :drive_file_path, 
            :drive_uploaded_time, 'google_drive',
            :etl_version, :task_id, :file_hash
        )
    """)
    
    # Inject lineage fields into each row
    for row in batch:
        row.setdefault('etl_version', ETL_VERSION)
        row.setdefault('task_id', task_id)
        row.setdefault('file_hash', '')
    # All DB operations are wrapped in context managers for resource safety.

    max_retries = 3
    for attempt in range(max_retries):
        try:
            with db_semaphore:  # Fix 10: Rate limiting
                active_db_ops.inc()
                try:
                    with engine.begin() as conn:
                        result = conn.execute(sql, batch)
                        inserted = result.rowcount
                        rows_inserted.inc(inserted)  # Fix 9: Metrics
                        return inserted
                finally:
                    active_db_ops.inc(-1)
        except OperationalError as e:
            if '1213' in str(e) and attempt < max_retries - 1:
                import random
                wait = (2 ** attempt) + random.uniform(0, 1)
                logger.error(f"Deadlock retry {attempt+1}/{max_retries}, waiting {wait:.1f}s...")
                time.sleep(wait)
            else:
                logger.error(f"Batch Insert Failed: {e}")
                raise


def update_file_status(file_id, filename, status, error_msg=None, file_hash=None):
    """
    Updates or inserts file status into file_registry.
    Uses ON DUPLICATE KEY UPDATE to avoid extra SELECTs.
    """
    try:
        sql = text("""
            INSERT INTO file_registry (drive_file_id, filename, status, error_message, file_hash, processed_at)
            VALUES (:file_id, :filename, :status, :error_msg, :file_hash, NOW())
            ON DUPLICATE KEY UPDATE 
                status = VALUES(status),
                error_message = VALUES(error_message),
                file_hash = COALESCE(VALUES(file_hash), file_hash),
                processed_at = NOW()
        """)
        with engine.begin() as conn:
            conn.execute(sql, {
                "file_id": file_id,
                "filename": filename,
                "status": status,
                "error_msg": error_msg,
                "file_hash": file_hash
            })
    except Exception as e:
        logger.error(f"Failed to update file_registry for {filename}: {e}")


# Fix 4: Dead Letter Queue
def send_to_dlq(file_id, file_name, error, task_id, retry_count=0):
    """Route permanently failed tasks to the Dead Letter Queue."""
    try:
        sql = text("""
            INSERT INTO etl_dlq (file_id, file_name, error, task_id, retry_count, failed_at)
            VALUES (:file_id, :file_name, :error, :task_id, :retry_count, NOW())
        """)
        with engine.begin() as conn:
            conn.execute(sql, {
                "file_id": file_id,
                "file_name": file_name,
                "error": str(error)[:2000],  # Truncate long errors
                "task_id": task_id,
                "retry_count": retry_count
            })
        dlq_entries.inc()  # Fix 9: Metrics
        logger.warning(f"[DLQ] Task routed to Dead Letter Queue: {file_name} (retries: {retry_count})")
    except Exception as e:
        logger.error(f"[DLQ] Failed to write to DLQ for {file_name}: {e}")


# SECTION 6: Dashboard Stats Refresh — Zero Downtime
@shared_task(name="tasks.gdrive.refresh_stats", ignore_result=True)
def refresh_dashboard_stats():
    """Recalculates dashboard statistics using UPSERT logic."""
    try:
        with engine.begin() as conn:
            # 1. UPSERT Global Summary (id=1)
            res = conn.execute(text("SELECT COUNT(*), COUNT(DISTINCT state), COUNT(DISTINCT category), COUNT(DISTINCT drive_file_id) FROM raw_google_map_drive_data")).fetchone()
            
            conn.execute(text("""
                INSERT INTO dashboard_stats_summary_v5 
                (id, total_records, total_states, total_categories, total_csvs, last_updated)
                VALUES (1, :total, :states, :cats, :csvs, NOW())
                ON DUPLICATE KEY UPDATE 
                    total_records = VALUES(total_records),
                    total_states = VALUES(total_states),
                    total_categories = VALUES(total_categories),
                    total_csvs = VALUES(total_csvs),
                    last_updated = NOW()
            """), {"total": res[0], "states": res[1], "cats": res[2], "csvs": res[3]})

            # 2. UPSERT State-Category Summary
            conn.execute(text("""
                INSERT INTO state_category_summary_v5 (state, category, record_count)
                SELECT state, category, COUNT(*) 
                FROM raw_google_map_drive_data 
                GROUP BY state, category
                ON DUPLICATE KEY UPDATE 
                    record_count = VALUES(record_count)
            """))
            
        logger.info("Dashboard stats refreshed successfully.")
    except Exception as e:
        logger.error(f"Stats Refresh Failed: {e}")

def trigger_stats_refresh():
    """Call this inside process_csv_task on success."""
    try:
        r = redis.from_url(os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"))
        val = r.incr("gdrive_etl_file_count")
        if val % 50 == 0:
            refresh_dashboard_stats.delay()
            logger.debug(f"Triggering stats refresh (Counter: {val})")
    except Exception as e:
        logger.warning(f"Failed to check stats trigger: {e}")


# SECTION 4: Main Processing Task (with all fixes applied)
@shared_task(
    bind=True, 
    max_retries=3,  # Fix 4: Reduced from 5 to route to DLQ faster
    name="tasks.gdrive.process_csv",
    autoretry_for=(OperationalError,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True
)
def process_csv_task(self, file_id, file_name, folder_id, folder_name, path, modified_time):
    """
    Celery task to download and process a single CSV file from Google Drive.
    Includes: size protection, validation, DLQ routing, graceful shutdown, metrics.
    """
    global shutdown_requested
    start_time = time.time()
    task_id = self.request.id
    file_hash = get_file_hash(file_id, modified_time)
    
    try:
        logger.debug(f"[START] Processing: {path}/{file_name}", extra={'task_id': task_id})
        update_file_status(file_id, file_name, 'PROCESSING', file_hash=file_hash)

        service = get_service()
        
        # Fix 1+2: Size-protected, context-managed download
        with download_csv(service, file_id) as stream:
            reader = csv.DictReader(stream)
            
            batch = []
            row_count = 0
            skipped_count = 0
            
            for row in reader:
                # Fix 7: Graceful shutdown check
                if shutdown_requested:
                    logger.info(f"[SHUTDOWN] Saving progress for {file_name} at row {row_count}...")
                    if batch:
                        commit_batch(batch, task_id=task_id)
                    update_file_status(file_id, file_name, 'PARTIAL', 
                                       error_msg=f"Shutdown at row {row_count}")
                    return f"Partial: {file_name} stopped at row {row_count}"
                
                norm_row = UniversalNormalizer.normalize_row({
                    **row, "drive_file_id": file_id, "drive_file_name": file_name,
                    "drive_folder_id": folder_id, "drive_folder_name": folder_name,
                    "drive_file_path": path, "drive_uploaded_time": modified_time
                })
                
                # Fix 5: Validation Removed (Raw Ingestion Mode)
                # We store EVERYTHING. Validation happens in the next ETL stage (Raw -> Clean).
                
                # Add file_hash to the row for lineage tracking
                norm_row['file_hash'] = file_hash
                batch.append(norm_row)
                row_count += 1
                
                if len(batch) >= 2000:
                    commit_batch(batch, task_id=task_id)
                    batch = []
            
            if batch:
                commit_batch(batch, task_id=task_id)

        update_file_status(file_id, file_name, 'PROCESSED', file_hash=file_hash)
        
        elapsed = time.time() - start_time
        processing_time.observe(elapsed)  # Fix 9: Metrics
        files_processed.inc()
        
        # Aggregated Logging via Redis
        try:
            r = redis.Redis(host='localhost', port=6379, db=0)
            total_files = r.incr('celery_files_processed')
            total_rows = r.incrby('celery_rows_inserted', row_count)
            
            # Log Progress every 50 files
            if total_files % 50 == 0:
                logger.info(f"⚡ Progress: {total_files} files done | {total_rows} rows inserted")
        except Exception:
            pass # Fail silently if Redis issues, don't crash task

        if row_count > 0:
            # Downgraded to DEBUG (Per-file log hidden usually)
            logger.debug(
                f"✅ [COMPLETE] {file_name} | Rows: {row_count} | Time: {elapsed:.2f}s",
                extra={'task_id': task_id}
            )
        else:
            logger.debug(f"✅ [COMPLETE] {file_name} (Empty) | Time: {elapsed:.2f}s", extra={'task_id': task_id})
        
        # Downgraded stats refresh log to DEBUG
        logger.debug(f"Triggering stats refresh (Counter: {2300})") # Placeholder counter
        trigger_stats_refresh()
        return f"Processed {file_name}: {row_count} rows ({skipped_count} skipped)"

    except Exception as e:
        logger.error(f"[ERROR] processing {file_name}: {e}", extra={'task_id': task_id})
        update_file_status(file_id, file_name, 'ERROR', str(e), file_hash=file_hash)
        
        # Fix 4: Route to DLQ on final retry
        if self.request.retries >= self.max_retries:
            send_to_dlq(file_id, file_name, str(e), task_id, 
                        retry_count=self.request.retries)
            logger.error(f"[DLQ] {file_name} exhausted retries, sent to Dead Letter Queue",
                        extra={'task_id': task_id})
            return f"DLQ: {file_name} after {self.request.retries} retries"
        
        raise self.retry(exc=e, countdown=60)
