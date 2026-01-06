from flask import Blueprint, jsonify
from model.master_table_model import MasterTable
from database.session import get_db_session

master_table_bp = Blueprint("master_table", __name__)

@master_table_bp.route("/master_table", methods=["GET"])
def get_master_table():
    """Fetch all master table rows"""
    session = get_db_session()
    print(session)
    try:
        rows = session.query(MasterTable).all()
        results = []
        for r in rows:
            results.append({
                "id": r.id,
                "global_business_id": r.global_business_id,
                "business_id": r.business_id,
                "business_name": r.business_name,
                "business_category": r.business_category,
                "business_subcategory": r.business_subcategory,
                "ratings": r.ratings,
                "primary_phone": r.primary_phone,
                "secondary_phone": r.secondary_phone,
                "other_phones": r.other_phones,
                "virtual_phone": r.virtual_phone,
                "whatsapp_phone": r.whatsapp_phone,
                "email": r.email,
                "website_url": r.website_url,
                "facebook_url": r.facebook_url,
                "linkedin_url": r.linkedin_url,
                "twitter_url": r.twitter_url,
                "address": r.address,
                "area": r.area,
                "city": r.city,
                "state": r.state,
                "pincode": r.pincode,
                "country": r.country,
                "latitude": r.latitude,
                "longitude": r.longitude,
                "avg_fees": r.avg_fees,
                "course_details": r.course_details,
                "duration": r.duration,
                "requirement": r.requirement,
                "avg_spent": r.avg_spent,
                "cost_for_two": r.cost_for_two,
                "reviews": r.reviews,
                "description": r.description,
                "data_source": r.data_source,
                "avg_salary": r.avg_salary,
                "admission_req_list": r.admission_req_list,
                "courses": r.courses,
                "created_at": r.created_at.isoformat() if r.created_at else None
            })
        return jsonify(results)
    finally:
        session.close()
