from sqlalchemy import text, create_engine
import os
import urllib.parse
from dotenv import load_dotenv
load_dotenv()

db_user = os.getenv("DB_USER")
db_pass = urllib.parse.quote_plus(os.getenv("DB_PASSWORD", "darshit@1912"))
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")

engine = create_engine(f'mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}')
tables = engine.connect().execute(text('SHOW TABLES')).fetchall()
for t in tables:
    print(t[0])
