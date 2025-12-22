from celery_app import celery
from services.csv_uploaders_listing.upload_shiksha import upload_shiksha_data
import os

@celery.task(bind=True,autoretry_for=(ValueError,RuntimeError),retry_kwargs={'max_retries':3,'countdown':5},retry_backoff=True,retry_jitter=True,acks_late=True,name='tasks.listings_task.upload_shiksha_task.process_shiksha_task')
def process_shiksha_task(self,file_paths):
    if not file_paths:
        raise ValueError("No file provided")
    result = upload_shiksha_data(file_paths)

    for path in file_paths:
        try:
            if os.path.exists(path):
                os.remove(path)
        except PermissionError:
            pass
    return result