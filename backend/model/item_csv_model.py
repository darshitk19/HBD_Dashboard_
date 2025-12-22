# model/items.py
from sqlalchemy import Column, Integer, String, Float, Text , BigInteger
from database.session import Base  # <-- app.database.session ke jagah sirf database.session

class ItemData(Base):
    __tablename__ = "item_data"

    id = Column(BigInteger, primary_key=True, index=True)
    category = Column(String(255))
    city = Column(String(500),primary_key=True)
    name = Column(String(255),primary_key=True)
    area = Column(String(255))
    address = Column(String(500))
    phone_no_1 = Column(String(255))
    phone_no_2 = Column(String(255))
    source = Column(String(500))
    ratings = Column(Float)
    sub_category = Column(String(500))
    state = Column(String(500))
    country = Column(String(500))
    email = Column(String(255))
    latitude = Column(Float)
    longitude = Column(Float)
    reviews = Column(String(500))
    facebook_url = Column(String(500))
    linkedin_url = Column(String(500))
    twitter_url = Column(String(500))
    description = Column(Text)
    pincode = Column(Integer)
    virtual_phone_no = Column(String(255))
    whatsapp_no = Column(String(255))
    phone_no_3 = Column(String(255))
    avg_spent = Column(Integer)
    cost_for_two = Column(Integer)
