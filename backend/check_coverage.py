"""
Compares Google Drive CSV files vs Database records to find missing files.
"""
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# --- Google Drive Setup ---
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts', 'gdrive_etl')
SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'honey-bee-digital-d96daf6e6faf.json')
ROOT_FOLDER_ID = "1ltTYjekxZsk2CdF20tSk1B2FnRn4119E"

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive.readonly']
)
service = build('drive', 'v3', credentials=creds, cache_discovery=False)

import urllib.parse
# --- Database Setup ---
DB_USER = os.getenv('DB_USER')
DB_PASS = urllib.parse.quote_plus(os.getenv('DB_PASSWORD') or "")
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

# --- Step 1: Count ALL CSV files in Google Drive (recursive) ---
print("📁 Scanning Google Drive for all CSV files... (this may take a few minutes)")
drive_files = {}  # {file_id: file_name}

def scan_folder(folder_id, path="ROOT"):
    page_token = None
    while True:
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, mimeType)",
            pageSize=1000,
            pageToken=page_token
        ).execute()
        
        for item in results.get('files', []):
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                scan_folder(item['id'], f"{path}/{item['name']}")
            elif item['name'].lower().endswith('.csv'):
                drive_files[item['id']] = f"{path}/{item['name']}"
        
        page_token = results.get('nextPageToken')
        if not page_token:
            break

scan_folder(ROOT_FOLDER_ID)
total_drive_csvs = len(drive_files)
print(f"✅ Google Drive: {total_drive_csvs} CSV files found")

# --- Step 2: Count distinct file_ids in database ---
with engine.connect() as conn:
    db_file_ids = set()
    res = conn.execute(text("SELECT DISTINCT drive_file_id FROM raw_google_map_drive_data")).fetchall()
    db_file_ids = {row[0] for row in res if row[0]}
    total_db_files = len(db_file_ids)
    
    # Total rows
    row_count = conn.execute(text("SELECT COUNT(*) FROM raw_google_map_drive_data")).scalar()

print(f"✅ Database: {total_db_files} unique files ingested ({row_count:,} total rows)")

# --- Step 3: Find missing files ---
drive_ids = set(drive_files.keys())
missing_ids = drive_ids - db_file_ids
extra_ids = db_file_ids - drive_ids

print(f"\n{'='*60}")
print(f"📊 COVERAGE REPORT")
print(f"{'='*60}")
print(f"  Google Drive CSVs : {total_drive_csvs}")
print(f"  Database Files    : {total_db_files}")
print(f"  Missing from DB   : {len(missing_ids)}")
print(f"  Coverage          : {(total_db_files / total_drive_csvs * 100) if total_drive_csvs else 0:.1f}%")
print(f"{'='*60}")

if missing_ids:
    print(f"\n⚠️  {len(missing_ids)} files NOT yet in database:")
    for i, fid in enumerate(list(missing_ids)[:20]):
        print(f"  {i+1}. {drive_files[fid]}")
    if len(missing_ids) > 20:
        print(f"  ... and {len(missing_ids) - 20} more")
else:
    print("\n🎉 ALL Google Drive CSV files are in the database!")
