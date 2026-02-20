import os
import re
import time
import threading
from urllib.parse import quote_plus
from dataclasses import dataclass, field
from typing import Optional
from pydantic import BaseModel, field_validator
import pandas as pd
import mysql.connector
from mysql.connector import Error
from playwright.sync_api import sync_playwright
from extensions import db  # Assuming db is initialized in extensions.py
# We will fix the Model import later, but for now assuming it will be in models folder
from model.scraper_task import ScraperTask 

def safe_filename(name: str) -> str:
    """Sanitize filename to remove/replace invalid characters."""
    name = name.strip().replace(' ', '_')
    return re.sub(r'[^\w\-]', '_', name)

class Business(BaseModel):
    """Pydantic model for business data validation"""
    name: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    phone_number: Optional[str] = None
    reviews_count: Optional[int] = None
    reviews_average: Optional[float] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    area: Optional[str] = None

    @field_validator('reviews_average')
    @classmethod
    def validate_rating(cls, v):
        if v is not None and (v < 0 or v > 5):
            raise ValueError('Rating must be between 0 and 5')
        return v

@dataclass
class BusinessList:
    business_list: list[Business] = field(default_factory=list)
    save_at: str = 'output'

    def dataframe(self):
        return pd.DataFrame([b.model_dump() for b in self.business_list])

    def save_to_excel(self, filename):
        if not os.path.exists(self.save_at):
            os.makedirs(self.save_at)
        self.dataframe().to_excel(f"{self.save_at}/{filename}.xlsx", index=False)

    def save_to_csv(self, filename):
        if not os.path.exists(self.save_at):
            os.makedirs(self.save_at)
        self.dataframe().to_csv(f"{self.save_at}/{filename}.csv", index=False)

    def save_to_mysql(self):
        """Save data to MySQL database using credentials from .env"""
        connection = None
        try:
            # print("Saving to DB:", os.getenv('DB_HOST'), os.getenv('DB_USER'), os.getenv('DB_NAME'))
            connection = mysql.connector.connect(
                host=os.getenv('DB_HOST'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME'),
                port=os.getenv('DB_PORT')
            )

            if connection.is_connected():
                cursor = connection.cursor()
                
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS google_Map (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(500),
                    address TEXT,
                    website VARCHAR(500),
                    phone_number VARCHAR(100),
                    reviews_count INT,
                    reviews_average FLOAT,
                    category VARCHAR(255),
                    subcategory VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(100),
                    area VARCHAR(500),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_business (name,address(255))
                )""")

                insert_query_complete_entries = """
                INSERT INTO google_Map (
                    name, address, website, phone_number,
                    reviews_count, reviews_average, category,
                    subcategory, city, state, area
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    website = VALUES(website),
                    phone_number = VALUES(phone_number),
                    reviews_count = VALUES(reviews_count),
                    reviews_average = VALUES(reviews_average),
                    subcategory = VALUES(subcategory),
                    area = VALUES(area)
                """

                for business in self.business_list:
                    cursor.execute(insert_query_complete_entries, (
                        business.name,
                        business.address,
                        business.website,
                        business.phone_number,
                        business.reviews_count,
                        business.reviews_average,
                        business.category,
                        business.subcategory,
                        business.city,
                        business.state,
                        business.area
                    ))

                connection.commit()
                print(f"✅ Successfully saved {len(self.business_list)} businesses to MySQL")

        except Error as e:
            print(f" MySQL Error: {e}")
        finally:
            if connection and connection.is_connected():
                connection.close()

def run_google_maps_scraper(task_id, app, search_list=None):
    """
    Runs the Playwright scraper.
    Note: We pass 'app' explicitly to handle Flask context inside the thread.
    """
    with app.app_context():
        task = ScraperTask.query.get(task_id)
        if not task: return

        if not search_list:
            # Search query parsing logic
            parts = task.search_query.split(' in ')
            cat = parts[0] if len(parts) > 0 else task.search_query
            search_list = [{"category": cat, "city": task.location, "state": ""}]

        start_from_index = task.last_index if task.last_index else 0
        task.status = "RUNNING"
        task.should_stop = False
        db.session.commit()

        with sync_playwright() as p:
            browser = None
            try:
                print(f"Starting DEEP Scraper for Task {task_id}...")
                browser = p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080}
                )
                page = context.new_page()

                for search_for_index, search_item in enumerate(search_list):
                    if search_for_index < start_from_index: continue

                    # --- STOP CHECK ---
                    db.session.refresh(task)
                    if task.should_stop:
                        task.status = "STOPPED"
                        db.session.commit()
                        return

                    category = search_item.get('category', '')
                    city = search_item.get('city', '')
                    
                    # Phase 1: Collect Links
                    encoded_query = quote_plus(f"{category} in {city}")
                    target_url = f"https://www.google.com/maps/search/{encoded_query}?hl=en"
                    
                    page.goto(target_url, timeout=60000)
                    
                    # Consent and Scroll
                    try:
                        page.wait_for_selector('div[role="feed"]', timeout=15000)
                    except: continue

                    for _ in range(10): # Scroll for links
                        if task.should_stop: break
                        page.mouse.wheel(0, 5000)
                        page.wait_for_timeout(1000)

                    all_links = page.locator('a[href*="/maps/place/"]').all()
                    business_urls = list(set([link.get_attribute('href') for link in all_links]))
                    
                    print(f"Task {task_id}: Found {len(business_urls)} links. Starting Phase 2...")
                    business_list = BusinessList()

                    # Phase 2: Detail Extraction
                    for i, url in enumerate(business_urls):
                        # --- GRANULAR STOP CHECK ---
                        db.session.refresh(task)
                        if task.should_stop:
                            task.status = "STOPPED"
                            task.last_index = search_for_index
                            business_list.save_to_mysql()
                            db.session.commit()
                            return

                        try:
                            page.goto(url, timeout=30000)
                            page.wait_for_timeout(1000)

                            # Extraction logic
                            name = page.locator('h1.DUwDvf').first.inner_text() if page.locator('h1.DUwDvf').count() > 0 else "Unknown"
                            
                            data = Business(
                                name=name, category=category, city=city, 
                                address=url # Fallback parsing
                            )
                            business_list.business_list.append(data)

                            # --- SYNC PROGRESS ---
                            task.total_found = i + 1
                            task.progress = int(((i + 1) / len(business_urls)) * 100)
                            db.session.commit()

                        except: continue

                    # Batch Save
                    business_list.save_to_mysql()
                    task.last_index = search_for_index + 1
                    db.session.commit()

                task.status = "COMPLETED"
                task.progress = 100
                db.session.commit()

            except Exception as e:
                print(f"Scraper Error: {e}")
                task.status = "ERROR"
                task.error_message = str(e)
                db.session.commit()
            finally:
                if browser: browser.close()