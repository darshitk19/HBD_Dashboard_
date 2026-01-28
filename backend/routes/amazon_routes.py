from flask import Blueprint, request, jsonify
import threading
from mysql.connector import Error
import mysql.connector
import os

# Import the service functions we created in Step 2
from services.scrapers.amazon_service import scrape_amazon_search, DB_CONFIG_AMAZON

amazon_api_bp = Blueprint('amazon_api_bp', __name__)

@amazon_api_bp.route('/api/amazon_products', methods=['GET'])
def get_amazon_products():
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG_AMAZON)
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM amazon_products LIMIT 1000")
        results = cursor.fetchall()
        return jsonify(results)
    except Error as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if connection and connection.is_connected():
            connection.close()

@amazon_api_bp.route('/api/scrape_amazon', methods=['POST'])
def scrape_and_insert():
    data = request.get_json()
    search_term = data.get('search_term')
    pages = int(data.get('pages', 1))
    
    if not search_term:
        return jsonify({'error': 'search_term is required'}), 400
        
    # Start the service in a thread
    thread = threading.Thread(target=scrape_amazon_search, args=(search_term, pages))
    thread.start()
    
    return jsonify({"status": "started"}), 202