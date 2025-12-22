from sqlalchemy import Column, Integer, String, Float, Text, JSON, TIMESTAMP
from sqlalchemy.sql import func
from database.session import Base

class AmazonProduct(Base):
    __tablename__ = "amazon_products"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    ASIN = Column(String(20), unique=True, nullable=False)
    Product_name = Column(Text, nullable=False)
    price = Column(String(50), nullable=False)
    rating = Column(Float, nullable=False)
    Number_of_ratings = Column(Integer, nullable=False)
    Brand = Column(String(255), nullable=False)
    Seller = Column(String(255), nullable=False)
    category = Column(String(255), nullable=False)
    subcategory = Column(String(255), nullable=False)
    sub_sub_category = Column(String(255), nullable=False)
    category_sub_sub_sub = Column(String(255), nullable=False)
    colour = Column(String(255), nullable=False)
    size_options = Column(Text, nullable=False)
    description = Column(Text, nullable=False)
    link = Column(Text, nullable=False)
    Image_URLs = Column(Text, nullable=False)
    About_the_items_bullet = Column(Text, nullable=False)
    Product_details = Column(JSON, nullable=False)
    Additional_Details = Column(JSON, nullable=False)
    Manufacturer_Name = Column(String(500), nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.now())
