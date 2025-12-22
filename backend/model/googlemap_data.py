from sqlalchemy import Column, Integer, String, Text, Float, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class GooglemapData(Base):
    __tablename__ = "businesses"   # use your actual table name

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String(500))
    address = Column(Text)
    website = Column(String(500))
    phone_number = Column(String(100))
    reviews_count = Column(Integer)
    reviews_average = Column(Float)
    category = Column(String(255))
    subcategory = Column(String(500))
    city = Column(String(100))
    state = Column(String(100))
    area = Column(String(500))
    created_at = Column(TIMESTAMP)
