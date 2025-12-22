from services.csv_uploaders_product import upload_amazon_products
from celery_app import celery
import os

@celery.task(bind=True,autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3,'countdown': 5},retry_jitter=True,acks_late=True)
def process_amazon_products_task(self,file_paths):
    if not file_paths:
        raise ValueError("No file paths provided to upload")
    try:
        return upload_amazon_products(file_paths)
    finally:
        for path in file_paths:
            if os.path.exists(path):
                os.remove(path)