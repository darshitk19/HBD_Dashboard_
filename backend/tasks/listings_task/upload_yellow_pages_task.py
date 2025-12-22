from services.csv_uploaders_listing.upload_yellow_pages import upload_yellow_pages_data
from celery_app import celery
import os

@celery.task(bind=True,autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3,'countdown': 5},retry_jitter=True,acks_late=True)
def process_yellow_pages_task(self,file_paths):
    if not file_paths:
        raise ValueError("No file provided")
    try:
        return upload_yellow_pages_data
    finally:
        for path in file_paths:
            if os.path.exists(path):
                os.remove(path)