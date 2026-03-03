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
    print("=" * 70)
    print("  PIPELINE STATUS")
    print("=" * 70)
    
    # Table counts
    raw = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).fetchone()[0]
    clean = conn.execute(text("SELECT COUNT(*) FROM raw_clean_google_map_data")).fetchone()[0]
    master = conn.execute(text("SELECT COUNT(*) FROM g_map_master_table")).fetchone()[0]
    print(f"  Raw Table     : {raw:,}")
    print(f"  Clean Table   : {clean:,}")
    print(f"  Master Table  : {master:,}")
    print()
    
    # File registry status
    print("  FILE REGISTRY STATUS")
    print("-" * 50)
    statuses = conn.execute(text("""
        SELECT status, COUNT(*) as cnt 
        FROM file_registry 
        GROUP BY status 
        ORDER BY cnt DESC
    """)).fetchall()
    for s in statuses:
        print(f"  {s[0]:<20} : {s[1]:,}")
    
    total_files = conn.execute(text("SELECT COUNT(*) FROM file_registry")).fetchone()[0]
    print(f"  {'TOTAL':<20} : {total_files:,}")
    print()
    
    # Folder registry status
    print("  FOLDER REGISTRY STATUS")
    print("-" * 50)
    total_folders = conn.execute(text("SELECT COUNT(*) FROM drive_folder_registry")).fetchone()[0]
    print(f"  Total Folders Scanned : {total_folders:,}")
    
    # Any IN_PROGRESS files?
    in_progress = conn.execute(text("SELECT COUNT(*) FROM file_registry WHERE status='IN_PROGRESS'")).fetchone()[0]
    error_files = conn.execute(text("SELECT COUNT(*) FROM file_registry WHERE status='ERROR'")).fetchone()[0]
    print(f"  Files IN_PROGRESS     : {in_progress:,}")
    print(f"  Files with ERROR      : {error_files:,}")
    print()
    
    # DLQ
    try:
        dlq = conn.execute(text("SELECT COUNT(*) FROM etl_dlq")).fetchone()[0]
        print(f"  Dead Letter Queue     : {dlq:,}")
    except:
        print(f"  Dead Letter Queue     : N/A")
    print()

    # Celery queue length
    try:
        import redis
        r = redis.from_url(os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"), socket_timeout=3)
        q_len = r.llen("celery")
        print(f"  Celery Queue Pending  : {q_len:,}")
    except Exception as e:
        print(f"  Celery Queue Pending  : Error ({e})")
    
    # Last processed validation ID vs max raw ID
    max_raw_id = conn.execute(text("SELECT MAX(id) FROM raw_google_map_drive_data")).fetchone()[0]
    last_val_id = conn.execute(text("SELECT meta_value FROM etl_metadata WHERE meta_key='last_processed_id'")).fetchone()
    last_val_id = int(last_val_id[0]) if last_val_id else 0
    pending_validation = max_raw_id - last_val_id if max_raw_id else 0
    print(f"\n  VALIDATION PROGRESS")
    print("-" * 50)
    print(f"  Max Raw ID            : {max_raw_id:,}")
    print(f"  Last Validated ID     : {last_val_id:,}")
    print(f"  Pending Validation    : {pending_validation:,}")
    print("=" * 70)
