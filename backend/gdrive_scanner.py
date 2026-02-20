import time
import logging
from tasks.gdrive_task.etl_tasks import get_service, process_csv_task

def gdrive_scanner_loop():
    service = get_service()
    logger = logging.getLogger("GDriveScanner")
    logger.info("GDrive scanner started.")
    last_checked = None  # TODO: Load from DB/metadata for persistence

    while True:
        try:
            # Example: List the 10 most recently modified CSV files
            results = service.files().list(
                q="mimeType='text/csv' and trashed=false",
                orderBy="modifiedTime desc",
                pageSize=10,
                fields="files(id, name, modifiedTime, parents)"
            ).execute()
            files = results.get('files', [])

            for file in files:
                file_id = file['id']
                file_name = file['name']
                modified_time = file['modifiedTime']
                folder_id = file.get('parents', [None])[0]
                folder_name = None  # Optional: fetch folder name if needed
                path = None  # Optional: build full path if needed
                # Submit to Celery
                process_csv_task.delay(file_id, file_name, folder_id, folder_name, path, modified_time)
                logger.info(f"Submitted {file_name} ({file_id}) for processing.")

            last_checked = time.time()
        except Exception as e:
            logger.error(f"GDrive scan error: {e}")

        time.sleep(60)  # Wait before next scan
