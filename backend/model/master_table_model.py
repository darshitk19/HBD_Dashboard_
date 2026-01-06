# model/master_model.py

from sqlalchemy import Column, Integer, String, Float, Text, JSON, TIMESTAMP
from sqlalchemy.sql import func
from extensions import db

class MasterTable(db.Model):
    __tablename__ = "master_table"

    id = Column(Integer, primary_key=True, autoincrement=True)
    global_business_id = Column(String(100), unique=True, nullable=False)
    business_id = Column(String(100), nullable=False)
    business_name= Column(String(255), nullable=False)
    business_category= Column(String(100), nullable=False)
    business_subcategory= Column(String(100))
    ratings= Column(Float, nullable= True)
    primary_phone = Column(String(20), nullable=False) #
    secondary_phone= Column(String(20), nullable=True)
    other_phones= Column(String(20), nullable=True)
    virtual_phone= Column(String(20), nullable=True)
    whatsapp_phone= Column(String(20), nullable=True)
    email= Column(String(255), nullable=False, unique= True)  #
    website_url= Column(String(255), nullable=True) #
    facebook_url=Column(String(255), nullable=True)
    linkedin_url=Column(String(255), nullable=True)
    twitter_url=Column(String(255), nullable=True)
    address=Column(String(255), nullable=False)
    area=Column(String(100), nullable=True)
    city=Column(String(50), nullable=False)
    state=Column(String(50), nullable=False)
    pincode= Column(String(10), nullable=False)
    country= Column(String(50), nullable=False, default='India') 
    latitude= Column(Float, nullable=True)
    longitude= Column(Float, nullable=True)
    avg_fees= Column(Float, nullable=True)   #
    course_details= Column(Text, nullable=True)
    duration= Column(String(100), nullable=True)
    requirement= Column(Text, nullable=True)
    avg_spent= Column(Float, nullable=True)
    cost_for_two= Column(Float, nullable=True)
    reviews= Column(Integer, nullable=True)  #
    description= Column(Text, nullable=True)
    data_source= Column(String(255), nullable=True)
    avg_salary= Column(Float, nullable=True)
    admission_req_list= Column(Text, nullable=True)
    courses= Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
