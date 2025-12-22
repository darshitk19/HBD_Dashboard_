from services.csv_uploaders_listing.upload_college_dunia import upload_college_dunia_data
from celery_app import celery
import os

@celery.task(bind=True,autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3,'countdown': 5},retry_jitter=True,acks_late=True)
def process_college_dunia_task(self,file_paths):
    
    if not file_paths:
        raise ValueError("No file paths provided for upload.")
    try:
        return upload_college_dunia_data(file_paths)
    finally:
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)