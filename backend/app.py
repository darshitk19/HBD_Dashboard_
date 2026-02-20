from gevent import monkey
monkey.patch_all()
from flask import request, jsonify, render_template, redirect,Flask
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import pathlib
from flask_cors import CORS
import os, sys, re, time, random, json, argparse, threading, csv, hashlib, signal
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dataclasses import dataclass, field
from dotenv import load_dotenv
from model.robust_gdrive_etl_v2 import start_background_etl
from pydantic import BaseModel, field_validator
from typing import Optional
from flask import Flask, request, jsonify
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
from flask_jwt_extended import verify_jwt_in_request  # ADDED MISSING IMPORT

# --- Config & Extensions ---
from config import Config
from extensions import db, jwt, cors, mail, migrate

# Initialize Flask app
# CORS is handled by extensions

app = Flask(__name__)

# Setup logging to file (rotates daily)
# Custom Formatter to strip ANSI codes and format consistently
class LogFormatter(logging.Formatter):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

    def format(self, record):
        # Create a copy to avoid modifying the original record
        record_copy = logging.makeLogRecord(record.__dict__)
        
        # Strip ANSI codes from the message
        if isinstance(record_copy.msg, str):
            record_copy.msg = self.ansi_escape.sub('', record_copy.msg)
            
        # Format levelname to be fixed width (8 chars) for alignment
        record_copy.levelname = f"{record_copy.levelname:<8}"
        
        return super().format(record_copy)

# Setup logging to file (rotates daily)
def setup_logging():
    log_dir = pathlib.Path(__file__).parent / 'logs' / 'flask'
    log_dir.mkdir(parents=True, exist_ok=True)
    log_filename = log_dir / f"flask_{datetime.now().strftime('%Y-%m-%d')}.log"

    # Define format: Date Time | Level | Logger | Message
    log_format = '%(asctime)s | %(levelname)s | %(name)-15s : %(message)s'
    
    # File Handler - Uses Custom Formatter (No Colors, Aligned)
    file_handler = TimedRotatingFileHandler(str(log_filename), when='midnight', backupCount=14, encoding='utf-8')
    file_formatter = LogFormatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)

    # Stream Handler - Standard Formatter (Colors allowed if supported, but we keep it simple)
    stream_handler = logging.StreamHandler()
    stream_formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
    stream_handler.setFormatter(stream_formatter)
    stream_handler.setLevel(logging.INFO)

    # Add handlers to root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplicates
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)
    
    # Silence third-party noise if needed
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.INFO) 
# CORS is handled by extensions
# --- Models ---
from model.user import User
from model.amazon_product_model import AmazonProduct
from model.googlemap_data import GoogleMapData
from model.item_csv_model import ItemData
from model.master_table_model import MasterTable
from model.raw_data_model import RawGoogleMap
from model.asklaila import Asklaila
from model.atm import ATM
from model.bank import Bank
from model.college_dunia import CollegeDunia

# --- Blueprints ---
from routes.auth_route import auth_bp
from routes.scraper_routes import scraper_bp
from routes.amazon_routes import amazon_api_bp
from routes.googlemap import googlemap_bp
from routes.master_table import master_table_bp
from routes.upload_product_csv import product_csv_bp
from routes.upload_item_csv import item_csv_bp
from routes.amazon_product import amazon_products_bp
from routes.items_data import item_bp
from routes.item_csv_download import item_csv_bp as item_csv_download_bp  # FIXED COLLISION
from routes.item_duplicate import item_duplicate_bp
from routes.gdrive_etl_routes.dashboard_stats import dashboard_bp
from routes.upload_others_csv import upload_others_csv_bp

# Listing Routes
from routes.listing_routes.upload_asklaila_route import asklaila_bp
from routes.listing_routes.upload_atm_route import atm_bp
from routes.listing_routes.upload_bank_route import bank_bp
from routes.listing_routes.upload_college_dunia_route import college_dunia_bp
from routes.listing_routes.upload_freelisting_route import freelisting_bp
from routes.listing_routes.upload_google_map_route import google_map_bp as gmap_upload_bp  # FIXED NAME
from routes.listing_routes.upload_google_map_scrape_route import google_map_scrape_bp
from routes.listing_routes.upload_heyplaces_route import heyplaces_bp
from routes.listing_routes.upload_justdial_route import justdial_bp
from routes.listing_routes.upload_magicpin_route import magicpin_bp
from routes.listing_routes.upload_nearbuy_route import nearbuy_bp
from routes.listing_routes.upload_pinda_route import pinda_bp
from routes.listing_routes.upload_post_office_route import post_office_bp
from routes.listing_routes.upload_schoolgis_route import schoolgis_bp
from routes.listing_routes.upload_shiksha_route import shiksha_bp
from routes.listing_routes.upload_yellow_pages_route import yellow_pages_bp

# Product Routes
from routes.product_routes.upload_amazon_products_route import amazon_bp as amazon_upload_bp # FIXED NAME
from routes.product_routes.upload_vivo_route import vivo_bp
from routes.product_routes.upload_big_basket_route import upload_big_basket_route as bigbasket_bp # FIXED NAME
from routes.product_routes.upload_blinkit_route import blinkit_bp
from routes.product_routes.upload_dmart_route import dmart_bp
from routes.product_routes.upload_flipkart_products_route import flipkart_bp
from routes.product_routes.upload_india_mart_route import indiamart_bp
from routes.product_routes.upload_jio_mart_route import jiomart_bp

# --- Initialize App ---
load_dotenv()
app = Flask(__name__)
app.config.from_object(Config)

# Init extensions
db.init_app(app)
migrate.init_app(app, db)
jwt.init_app(app)
cors.init_app(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)
mail.init_app(app)

with app.app_context():
    db.create_all()
    # Run Safe Migrations (Indexes & Schema Updates)
    from utils.db_migrations import run_pending_migrations
    run_pending_migrations(app)


# --- GLOBAL JWT PROTECTION ---
PUBLIC_ROUTES = [
    "/", 
    "/auth/signup", 
    "/auth/login", 
    "/auth/logout",
    "/auth/forgot-password", 
    "/auth/verify-otp", 
    "/auth/reset-password", 
    "/health",
    "/api/master-dashboard-stats",
    "/atm/fetch-data",
    "/api/bank/fetch-data",
    "/asklaila/fetch-data",
    "/college-dunia/fetch-data",
    "/api/google-listings", 
]

@app.before_request
def protect_all_routes():
    if request.method == "OPTIONS":
        return jsonify({"message": "CORS preflight successful"}), 200
        
    normalized_path = request.path.rstrip('/')
    normalized_public_routes = [route.rstrip('/') for route in PUBLIC_ROUTES]
    
    if normalized_path in normalized_public_routes or request.path in PUBLIC_ROUTES:
        return None
    
    try:
        verify_jwt_in_request()
    except Exception as e:
        print(f"❌ JWT REJECTED for {request.path}: {str(e)}") 
        return jsonify({"message": "Missing or invalid token", "error": str(e)}), 401


# --- Register Blueprints ---
app.register_blueprint(auth_bp, url_prefix="/auth")
app.register_blueprint(scraper_bp, url_prefix="/api")
app.register_blueprint(amazon_api_bp, url_prefix="/api")
app.register_blueprint(googlemap_bp, url_prefix='/api') 
app.register_blueprint(master_table_bp)
app.register_blueprint(product_csv_bp)
app.register_blueprint(item_csv_bp)
app.register_blueprint(amazon_products_bp)
app.register_blueprint(item_bp, url_prefix="/items")
app.register_blueprint(item_csv_download_bp)
app.register_blueprint(item_duplicate_bp)
app.register_blueprint(upload_others_csv_bp)
app.register_blueprint(dashboard_bp)

# MISSING IMPORT: You tried to register this but it wasn't imported anywhere.
# app.register_blueprint(listing_master_bp, url_prefix="/api")

# Register Listing & Product Blueprints (Batch)
blueprints_listing = [
    (asklaila_bp, "/asklaila"), (atm_bp, "/atm"), (bank_bp, "/bank"),
    (college_dunia_bp, "/college-dunia"), (freelisting_bp, "/freelisting"),
    (gmap_upload_bp, "/google-map"), (google_map_scrape_bp, "/google-map-scrape"),
    (heyplaces_bp, "/heyplaces"), (justdial_bp, "/justdial"), (magicpin_bp, "/magicpin"),
    (nearbuy_bp, "/nearbuy"), (pinda_bp, "/pinda"), (post_office_bp, "/post-office"),
    (schoolgis_bp, "/schoolgis"), (shiksha_bp, "/shiksha"), (yellow_pages_bp, "/yellow-pages"),
    (amazon_upload_bp, "/amazon"), (vivo_bp, "/vivo"), (blinkit_bp, "/blinkit"),
    (dmart_bp, "/dmart"), (flipkart_bp, "/flipkart"), (indiamart_bp, "/india-mart"),
    (jiomart_bp, "/jio-mart"), (bigbasket_bp, "/big-basket")
]

for bp, prefix in blueprints_listing:
    app.register_blueprint(bp, url_prefix=prefix)


# --- Basic Routes ---
@app.route('/')
def index():
    return jsonify({"message": "Flask API is running! Clean and Modular."})


# --- BACKGROUND TASKS & SCRAPER LOGIC ---
def run_gdrive_ingestion_loop():
    try:
        from model.robust_gdrive_etl_v2 import GDriveHighSpeedIngestor
        ingestor = GDriveHighSpeedIngestor()
        while True:
            try:
                ingestor.run_pipeline()
                time.sleep(5)
            except Exception as e:
                print(f"❌ ETL Loop Error: {e}")
                time.sleep(5)
    except Exception as e:
        print(f"CRITICAL: Failed to initialize GDrive Parallel ETL: {e}")
        import traceback
        traceback.print_exc()

def safe_filename(name: str) -> str:
    name = name.strip().replace(' ', '_')
    return re.sub(r'[^\w\-]', '_', name)

class Business(BaseModel):
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
        connection = None
        try:
            print("Saving to DB:", os.getenv('DB_HOST'), os.getenv('DB_USER'), os.getenv('DB_NAME'))
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
                    name VARCHAR(500), address TEXT, website VARCHAR(500),
                    phone_number VARCHAR(100), reviews_count INT,
                    reviews_average FLOAT, category VARCHAR(255),
                    subcategory VARCHAR(500), city VARCHAR(100),
                    state VARCHAR(100), area VARCHAR(500),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_business (name,address)
                )""")

                insert_query = """
                INSERT INTO google_Map (
                    name, address, website, phone_number,
                    reviews_count, reviews_average, category,
                    subcategory, city, state, area
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # this will be used during the handling of duplicate entries
                # ON DUPLICATE KEY UPDATE
                #     website = VALUES(website),
                #     phone_number = VALUES(phone_number),
                #     reviews_count = VALUES(reviews_count),
                #     reviews_average = VALUES(reviews_average),
                #     subcategory = VALUES(subcategory),
                #     area = VALUES(area)

                insert_query_incomplete_entries = """
                INSERT INTO businesses_incomplete (
                    name, address, website, phone_number,
                    reviews_count, reviews_average, category,
                    subcategory, city, state, area
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                insert_query_duplicate_entries = """
                INSERT INTO businesses_duplicates (
                    name, address, website, phone_number,
                    reviews_count, reviews_average, category,
                    subcategory, city, state, area
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    website = VALUES(website), phone_number = VALUES(phone_number),
                    reviews_count = VALUES(reviews_count), reviews_average = VALUES(reviews_average),
                    subcategory = VALUES(subcategory), area = VALUES(area)
                """

                for business in self.business_list:
                    cursor.execute(insert_query, (
                        business.name, business.address, business.website, business.phone_number,
                        business.reviews_count, business.reviews_average, business.category,
                        business.subcategory, business.city, business.state, business.area
                    ))
                connection.commit()
                print(f"✅ Successfully saved {len(self.business_list)} businesses to MySQL")

        except Error as e:
            print(f" MySQL Error: {e}")
        finally:
            if connection and connection.is_connected():
                connection.close()

def run_scraper(search_list):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto("https://www.google.com/maps", timeout=60000)
        page.wait_for_timeout(5000)

        for search_for_index, search_item in enumerate(search_list):
            category = search_item['category']
            city = search_item['city']
            state = search_item['state']
            search_query = f"{category}, {city}, {state}"
            print(f"-----\n{search_for_index} - {search_query}")

            page.locator('//input[@id="searchboxinput"]').fill(search_query)
            page.wait_for_timeout(3000)
            page.keyboard.press("Enter")
            page.wait_for_timeout(5000)

            page.hover('//a[contains(@href, "https://www.google.com/maps/place")]')

            previously_counted = 0
            while True:
                page.mouse.wheel(0, 10000)
                page.wait_for_timeout(3000)
                current_count = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').count()

                if current_count >= 1000:
                    listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()[:1000]
                    listings = [listing.locator("xpath=..") for listing in listings]
                    print(f"Total Scraped: {len(listings)}")
                    break
                elif current_count == previously_counted:
                    listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()
                    print(f"Arrived at all available\nTotal Scraped: {len(listings)}")
                    break
                else:
                    previously_counted = current_count
                    print(f"Currently Scraped: {current_count}")

            business_list = BusinessList()

            for listing in listings:
                try:
                    listing.click()
                    page.wait_for_timeout(5000)

                    name_attr = 'aria-label'
                    address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
                    website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
                    phone_xpath = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
                    review_count_xpath = '//button[@jsaction="pane.reviewChart.moreReviews"]//span'
                    reviews_avg_xpath = '//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]'
                    subcategory_xpath = '//div[contains(@aria-label, "stars")]/following-sibling::div[contains(@class, "fontBodyMedium")]'

                    business_data = {
                        'name': listing.get_attribute(name_attr) or "",
                        'address': page.locator(address_xpath).nth(0).inner_text() if page.locator(address_xpath).count() > 0 else "",
                        'website': page.locator(website_xpath).nth(0).inner_text() if page.locator(website_xpath).count() > 0 else "",
                        'phone_number': page.locator(phone_xpath).nth(0).inner_text() if page.locator(phone_xpath).count() > 0 else "",
                        'subcategory': page.locator(subcategory_xpath).nth(0).inner_text() if page.locator(subcategory_xpath).count() > 0 else "",
                        'category': category,
                        'city': city,
                        'state': state
                    }

                    if page.locator(review_count_xpath).count() > 0:
                        business_data['reviews_count'] = int(page.locator(review_count_xpath).inner_text().split()[0].replace(',', '').strip())
                    else:
                        business_data['reviews_count'] = None

                    if page.locator(reviews_avg_xpath).count() > 0:
                        business_data['reviews_average'] = float(page.locator(reviews_avg_xpath).get_attribute(name_attr).split()[0].replace(',', '.').strip())
                    else:
                        business_data['reviews_average'] = None

                    if business_data['address']:
                        addr_parts = business_data['address'].split(',')
                        business_data['area'] = ','.join(addr_parts[:2]).strip() if len(addr_parts) >= 2 else addr_parts[0].strip()
                    else:
                        business_data['area'] = ""

                    business_list.business_list.append(Business(**business_data))
                except Exception as e:
                    print(f"Error occurred during map parsing: {e}")

            safe_name = safe_filename(f"google_maps_data_{category}_{city}_{state}")
            try:
                business_list.save_to_csv(safe_name)
            except Exception as e:
                print("CSV Save Error:", e)

            try:
                business_list.save_to_mysql()
            except Exception as e:
                print("MySQL Save Error:", e)

        browser.close()


DB_CONFIG_AMAZON = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD_PLAIN') or os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': os.getenv('DB_PORT')
}
ua = UserAgent()
BASE_URL = 'https://www.amazon.in'

def get_headers():
    return {
        'User-Agent': ua.random,
        'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.amazon.in/'
    }

def get_product_details(url):
    try:
        time.sleep(random.uniform(1, 3))
        response = requests.get(url, headers=get_headers())
        response.raise_for_status()
        if 'captcha' in response.text.lower():
            raise Exception("Captcha page encountered")
        soup = BeautifulSoup(response.text, 'html.parser')
        asin = url.split('/dp/')[1].split('/')[0] if '/dp/' in url else None
        
        title_selectors = ["#productTitle", "span.a-size-large.product-title-word-break", "h1.a-size-large.a-spacing-none", "span#title", "h1#title"]
        name = next((soup.select_one(s).get_text().strip() for s in title_selectors if soup.select_one(s)), None)
        
        price = soup.select_one('.a-price-whole')
        price = '₹' + price.get_text().strip().replace(',', '') if price else None
        
        rating = soup.select_one('span[data-asin][class*="a-icon-alt"]') or soup.select_one('.a-icon-alt')
        rating = float(rating.get_text().split()[0]) if rating else None
        
        num_ratings = soup.select_one('#acrCustomerReviewText')
        num_ratings = int(num_ratings.get_text().split()[0].replace(',', '')) if num_ratings else 0
        
        brand = soup.select_one('#bylineInfo')
        brand = brand.get_text().strip() if brand else None

        return {
            'ASIN': asin, 'Product_name': name, 'price': price, 'rating': rating,
            'Number_of_ratings': num_ratings, 'Brand': brand, 'Seller': None,
            'category': None, 'subcategory': None, 'sub_sub_category': None,
            'category_sub_sub_sub': None, 'colour': None, 'size_options': None,
            'description': None, 'link': url, 'Image_URLs': None,
            'About_the_items_bullet': None, 'Product_details': json.dumps({}),
            'Additional_Details': json.dumps({}), 'Manufacturer_Name': None
        }
    except Exception as e:
        print(f"Error scraping product: {e}")
        return None

def insert_products_to_db(products):
    if not products: return 0
    connection = None
    inserted = 0
    try:
        connection = mysql.connector.connect(**DB_CONFIG_AMAZON)
        cursor = connection.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS amazon_products (
            id INT AUTO_INCREMENT PRIMARY KEY, ASIN VARCHAR(20) UNIQUE,
            Product_name TEXT, price VARCHAR(50), rating FLOAT,
            Number_of_ratings INT, Brand VARCHAR(255), Seller VARCHAR(255),
            category VARCHAR(255), subcategory VARCHAR(255), sub_sub_category VARCHAR(255),
            category_sub_sub_sub VARCHAR(255), colour VARCHAR(255), size_options TEXT,
            description TEXT, link TEXT, Image_URLs TEXT, About_the_items_bullet TEXT,
            Product_details JSON, Additional_Details JSON, Manufacturer_Name VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        insert_query = '''
        INSERT INTO amazon_products (ASIN, Product_name, price, rating, Number_of_ratings, Brand, Seller, category, subcategory, sub_sub_category, category_sub_sub_sub, colour, size_options, description, link, Image_URLs, About_the_items_bullet, Product_details, Additional_Details, Manufacturer_Name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Product_name=VALUES(Product_name), price=VALUES(price), rating=VALUES(rating),
            Number_of_ratings=VALUES(Number_of_ratings), Brand=VALUES(Brand)
        '''
        for product in products:
            cursor.execute(insert_query, (
                product['ASIN'], product['Product_name'], product['price'], product['rating'],
                product['Number_of_ratings'], product['Brand'], product['Seller'], product['category'],
                product['subcategory'], product['sub_sub_category'], product['category_sub_sub_sub'],
                product['colour'], product['size_options'], product['description'], product['link'],
                product['Image_URLs'], product['About_the_items_bullet'], product['Product_details'],
                product['Additional_Details'], product['Manufacturer_Name']
            ))
            inserted += 1
        connection.commit()
    except Error as e:
        print(f"Error inserting products: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()
    return inserted

def scrape_amazon_search(search_term, pages=1, limit=1000):
    products = []
    previous_count = 0
    for page in range(1, pages + 1):
        try:
            print(f"\n----- Scraping Page {page} -----")
            search_url = f"{BASE_URL}/s?k={requests.utils.quote(search_term)}&page={page}"
            time.sleep(random.uniform(1, 2))
            response = requests.get(search_url, headers=get_headers())
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            product_links = [urljoin(BASE_URL, a['href']) for a in soup.select('a.a-link-normal.s-no-outline') if a.get('href')]
            if len(product_links) == previous_count: break
            previous_count = len(product_links)

            for link in product_links:
                if len(products) >= limit: break
                product_data = get_product_details(link)
                if product_data: products.append(product_data)
                time.sleep(random.uniform(1.5, 4))
                
            if len(products) >= limit: break
        except Exception as e:
            time.sleep(random.uniform(5, 10))
            continue

    if products:
        os.makedirs("output", exist_ok=True)
        filename = f"output/amazon_data_{search_term.replace(' ', '_')}.csv"
        with open(filename, "w", newline="", encoding="utf-8") as f:
            dict_writer = csv.DictWriter(f, products[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(products)
            
    insert_products_to_db(products)
    return products

@app.route('/api/scrape', methods=['POST'])
def api_scrape():
    data = request.json
    if not data or 'queries' not in data:
        return jsonify({'error': 'Missing queries'}), 400
    
    search_list = []
    for line in data['queries']:
        parts = [part.strip() for part in line.split(',')]
        if len(parts) == 3:
            search_list.append({"category": parts[0], "city": parts[1], "state": parts[2]})
    
    if not search_list: return jsonify({'error': 'No valid queries provided'}), 400

    threading.Thread(target=run_scraper, args=(search_list,)).start()
    return jsonify({'status': 'Scraping started', 'searches': len(search_list)}), 202

@app.route('/api/results', methods=['GET'])
def api_results():
    connection = None
    try:
        connection = mysql.connector.connect(
           host=os.getenv('DB_HOST'), user=os.getenv('DB_USER'),
           password=os.getenv('DB_PASSWORD'), database=os.getenv('DB_NAME'), port=os.getenv('DB_PORT')
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM google_Map LIMIT 1000")
        return jsonify(cursor.fetchall())
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected(): connection.close()

@app.route('/api/amazon_products', methods=['GET'])
def get_amazon_products():
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG_AMAZON)
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM amazon_products LIMIT 1000")
        return jsonify(cursor.fetchall())
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected(): connection.close()

@app.route('/api/scrape_amazon', methods=['POST'])
def scrape_and_insert():
    data = request.get_json()
    search_term = data.get('search_term')
    if not search_term: return jsonify({'error': 'search_term is required'}), 400
    threading.Thread(target=scrape_amazon_search, args=(search_term, int(data.get('pages', 1)))).start()
    return jsonify({"status": "started"}), 202

@app.route('/upload_amazon_products_data', methods=["POST"])
def upload_amazon_products_data():
    return jsonify({"status": "error", "message": "Logic missing in original code"}), 400

# --- MAIN BLOCK ---
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Amazon Scraper API & CLI')
    parser.add_argument('--runserver', action='store_true', help='Run the Flask API server')
    parser.add_argument('--scrape', type=str, help='Search term to scrape from Amazon')
    parser.add_argument('--pages', type=int, default=1, help='Number of pages to scrape (default: 1)')
    parser.add_argument('--show', action='store_true', help='Show product data as a table after scraping')
    parser.add_argument("-s", "--search", type=str, help="Google Map Category to search")
    parser.add_argument("-t", "--total", type=int, help="Total items to search")
    args = parser.parse_args()

    # Setup logging only if running as main application
    setup_logging()

    if args.runserver:
        app.debug = True  # Set debug early so the check below sees it
    
    # Start thread
    # Start thread
    print("Loaded DB:", os.getenv("DB_USER"), os.getenv("DB_NAME"))
    
    # Start background ETL thread
    print("🔗 Starting Background Sync Thread...")
    ingestor = start_background_etl()
    
    # Register Signal Handler for Graceful Shutdown
    def signal_handler(sig, frame):
        print('\n🛑 shutdown signal received. Stopping background threads...')
        if ingestor:
            ingestor.shutdown()
        # Allow Flask to shutdown gracefully if needed, or just exit
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    # threading.Thread(target=run_gdrive_ingestion_loop, daemon=True).start()

    if args.runserver:
        app.run(debug=True, use_reloader=False)
        
    elif args.scrape:
        print(f"Scraping Amazon for '{args.scrape}' ({args.pages} pages)...")
        scraped_products = scrape_amazon_search(args.scrape, args.pages)
        if args.show:
            try:
                connection = mysql.connector.connect(**DB_CONFIG_AMAZON)
                cursor = connection.cursor(dictionary=True)
                cursor.execute("SELECT * FROM amazon_products ORDER BY created_at DESC LIMIT 20")
                results = cursor.fetchall()
                if results:
                    print(pd.DataFrame(results).to_string(index=False))
                else:
                    print("No products found in the database.")
            except Error as e:
                print(f"Error fetching products: {e}")
            finally:
                if connection and connection.is_connected(): connection.close()

    elif args.search:
        # Replaces the broken `main()` Google map scraping execution logic
        run_scraper([{"category": args.search, "city": "", "state": ""}])
        
    else:
        parser.print_help()