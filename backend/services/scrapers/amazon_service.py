import os
import time
import random
import json
import requests
import csv
import threading
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from fake_useragent import UserAgent
import mysql.connector
from mysql.connector import Error

# We define DB Config here again or import from config
DB_CONFIG_AMAZON = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
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
        'Referer': 'https://www.amazon.in/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1'
    }

def get_product_details(url):
    print("Inside get_product_details:", url)
    try:
        time.sleep(random.uniform(1, 3))
        response = requests.get(url, headers=get_headers())
        response.raise_for_status()
        if 'captcha' in response.text.lower():
            raise Exception("Captcha page encountered")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # ASIN Logic
        asin = None
        if '/dp/' in url:
             asin = url.split('/dp/')[1].split('/')[0].split('?')[0]
        elif '/gp/product/' in url:
             asin = url.split('/gp/product/')[1].split('/')[0].split('?')[0]

        # Title Logic
        title_selectors = ["#productTitle", "span.a-size-large", "span#title"]
        name = None
        for selector in title_selectors:
            elem = soup.select_one(selector)
            if elem:
                name = elem.get_text().strip()
                break

        # Price Logic
        price_elem = soup.select_one('.a-price-whole')
        price = '₹' + price_elem.get_text().strip().replace(',', '') if price_elem else None
        
        # Rating Logic
        rating_elem = soup.select_one('span[data-asin][class*="a-icon-alt"]')
        if not rating_elem: rating_elem = soup.select_one('.a-icon-alt')
        rating = rating_elem.get_text().split()[0] if rating_elem else None
        
        # Num Ratings Logic
        num_ratings_elem = soup.select_one('#acrCustomerReviewText')
        if num_ratings_elem:
            raw_text = num_ratings_elem.get_text()
            clean_num = ''.join(filter(str.isdigit, raw_text))
            num_ratings = int(clean_num) if clean_num else 0
        else:
            num_ratings = 0

        brand_elem = soup.select_one('#bylineInfo')
        brand = brand_elem.get_text().strip() if brand_elem else None

        return {
            'ASIN': asin,
            'Product_name': name,
            'price': price,
            'rating': float(rating) if rating else None,
            'Number_of_ratings': num_ratings,
            'Brand': brand,
            'Seller': None, 'category': None, 'subcategory': None,
            'sub_sub_category': None, 'category_sub_sub_sub': None,
            'colour': None, 'size_options': None, 'description': None,
            'link': url, 'Image_URLs': None, 'About_the_items_bullet': None,
            'Product_details': json.dumps({}), 
            'Additional_Details': json.dumps({}),
            'Manufacturer_Name': None
        }
    except Exception as e:
        print(f"Error scraping product details: {e}")
        return None

def insert_products_to_db(products):
    if not products:
        return 0
    connection = None
    inserted = 0
    try:
        connection = mysql.connector.connect(**DB_CONFIG_AMAZON)
        cursor = connection.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS amazon_products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ASIN VARCHAR(20) UNIQUE,
            Product_name TEXT,
            price VARCHAR(50),
            rating FLOAT,
            Number_of_ratings INT,
            Brand VARCHAR(255),
            Seller VARCHAR(255),
            category VARCHAR(255),
            subcategory VARCHAR(255),
            sub_sub_category VARCHAR(255),
            category_sub_sub_sub VARCHAR(255),
            colour VARCHAR(255),
            size_options TEXT,
            description TEXT,
            link TEXT,
            Image_URLs TEXT,
            About_the_items_bullet TEXT,
            Product_details JSON,
            Additional_Details JSON,
            Manufacturer_Name VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )'''
        cursor.execute(create_table_query)
        insert_query = '''
        INSERT INTO amazon_products (
            ASIN, Product_name, price, rating, Number_of_ratings, Brand, Seller, category, subcategory, sub_sub_category, category_sub_sub_sub, colour, size_options, description, link, Image_URLs, About_the_items_bullet, Product_details, Additional_Details, Manufacturer_Name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Product_name=VALUES(Product_name),
            price=VALUES(price),
            rating=VALUES(rating),
            Number_of_ratings=VALUES(Number_of_ratings),
            Brand=VALUES(Brand),
            Seller=VALUES(Seller),
            category=VALUES(category),
            subcategory=VALUES(subcategory),
            sub_sub_category=VALUES(sub_sub_category),
            category_sub_sub_sub=VALUES(category_sub_sub_sub),
            colour=VALUES(colour),
            size_options=VALUES(size_options),
            description=VALUES(description),
            link=VALUES(link),
            Image_URLs=VALUES(Image_URLs),
            About_the_items_bullet=VALUES(About_the_items_bullet),
            Product_details=VALUES(Product_details),
            Additional_Details=VALUES(Additional_Details),
            Manufacturer_Name=VALUES(Manufacturer_Name)
        '''
        for product in products:
            cursor.execute(insert_query, (
                product['ASIN'],
                product['Product_name'],
                product['price'],
                product['rating'],
                product['Number_of_ratings'],
                product['Brand'],
                product['Seller'],
                product['category'],
                product['subcategory'],
                product['sub_sub_category'],
                product['category_sub_sub_sub'],
                product['colour'],
                product['size_options'],
                product['description'],
                product['link'],
                product['Image_URLs'],
                product['About_the_items_bullet'],
                product['Product_details'],
                product['Additional_Details'],
                product['Manufacturer_Name']
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

            product_links = [
                urljoin(BASE_URL, a['href'])
                for a in soup.select('a.a-link-normal.s-no-outline')
                if a.get('href')
            ]

            print("Found links on this page:", len(product_links))

            if len(product_links) == previous_count:
                print("No new product links appearing… stopping scraping.")
                break

            previous_count = len(product_links)

            for link in product_links:
                if len(products) >= limit:
                    break

                print("Fetching product:", link)
                product_data = get_product_details(link)

                if product_data:
                    products.append(product_data)

                time.sleep(random.uniform(1.5, 4))

            if len(products) >= limit:
                break

        except Exception as e:
            print(f"Error on page {page}: {str(e)}")
            time.sleep(random.uniform(5, 10))
            continue

    print("Saving data to MySQL…")
    inserted = insert_products_to_db(products)
    if inserted:
        print(f"Inserted {inserted} rows into DB.")
    else:
        print("No data inserted into DB.")

    return products