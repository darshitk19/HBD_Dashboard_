from flask import Blueprint, jsonify
from model.googlemap_data import GooglemapData
from database.session import get_db_session

googlemap_bp = Blueprint("googlemap", __name__)

# -----------------
# API Endpoints
# -----------------
@googlemap_bp.route("/googlemap_data", methods=["GET"])
def get_places():
    """Fetch all places"""
    session = get_db_session()
    try:
        places = session.query(GooglemapData).all()
        results = []
        for p in places:
            results.append({
                "id": p.id,
                "name": p.name,
                "address": p.address,
                "website": p.website,
                "phone_number": p.phone_number,
                "reviews_count": p.reviews_count,
                "reviews_average": p.reviews_average,
                "category": p.category,
                "subcategory": p.subcategory,
                "city": p.city,
                "state": p.state,
                "area": p.area,
                "created_at": p.created_at.isoformat() if p.created_at else None
            })
        return jsonify(results)
    finally:
        session.close()
