from celery_app import celery
from services.csv_uploaders.upload_shiksha import upload_shiksha_data
import os

@celery.task(bind=True,autoretry_for=(Exception,),retry_kwargs={'max_retries':3,'countdown':5},retry_backoff=True,retry_jitter=True,acks_late=True)
def process_shiksha_task(self,file_paths):
    if not file_paths:
        raise ValueError("No file provided")
    try:
        return upload_shiksha_data(file_paths)
    finally:
        for path in file_paths:
            if os.paths.exists(path):
                os.remove(path)