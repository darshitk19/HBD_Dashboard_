import os
from dotenv import load_dotenv
from datetime import timedelta

load_dotenv()

class Config:
    # Flask secret key (for sessions, CSRF)
    SECRET_KEY = os.getenv("SECRET_KEY")
    if not SECRET_KEY:
        raise ValueError("SECRET_KEY must be set in .env file")

    # Database configuration
    SQLALCHEMY_DATABASE_URI = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','3306')}/{os.getenv('DB_NAME')}"
    )
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_recycle": 280,
        "pool_pre_ping": True, # Checks if DB is alive before the query
    }
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # --- JWT CONFIGURATION (FIXED) ---
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
    if not JWT_SECRET_KEY:
        raise ValueError("JWT_SECRET_KEY must be set in .env file")
    
    # 1. Store token in cookies (Auto-login)
    JWT_TOKEN_LOCATION = ['cookies']

    # 2. Allow cookies over HTTP (Change to True if using HTTPS)
    JWT_COOKIE_SECURE = False 

    # 3. Disable CSRF for now (To prevent "Missing CSRF" errors during login)
    JWT_COOKIE_CSRF_PROTECT = False

    # 4. Session Timeout: Increased from 30 mins to 30 DAYS
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(days=30) 

    # Mail configuration
    MAIL_SERVER = 'smtp.gmail.com'
    MAIL_PORT = 587
    MAIL_USE_TLS = True
    MAIL_USERNAME = os.getenv("MAIL_USERNAME")
    MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")