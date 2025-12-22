from flask import Blueprint, jsonify, request
from sqlalchemy import or_, and_
from database.session import SessionLocal
from model.item_csv_model import ItemData

item_bp = Blueprint("items", __name__)

# ✅ helper
def serialize(item):
    data = item.__dict__.copy()
    data.pop("_sa_instance_state", None)
    return data

# ✅ utility for pagination
def paginate(query, page, limit):
    total = query.count()
    items = query.offset((page - 1) * limit).limit(limit).all()
    return {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total // limit) + (1 if total % limit else 0),
        "items": [serialize(item) for item in items]
    }

# ✅ Complete Items API with pagination
@item_bp.route("/complete", methods=["GET"])
def get_complete_items():
    db = SessionLocal()
    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 1000))

        query = db.query(ItemData).filter(
            # name required
            ItemData.name.isnot(None), ItemData.name != "",
            # at least one phone required
            or_(
                (ItemData.phone_no_1.isnot(None) & (ItemData.phone_no_1 != "")),
                (ItemData.phone_no_2.isnot(None) & (ItemData.phone_no_2 != "")),
                (ItemData.phone_no_3.isnot(None) & (ItemData.phone_no_3 != "")),
                (ItemData.whatsapp_no.isnot(None) & (ItemData.whatsapp_no != "")),
                (ItemData.virtual_phone_no.isnot(None) & (ItemData.virtual_phone_no != ""))
            ),
            # category required
            ItemData.category.isnot(None), ItemData.category != "",
            # sub category required
            ItemData.sub_category.isnot(None), ItemData.sub_category != "",
            # area required
            ItemData.area.isnot(None), ItemData.area != "",
            # city required
            ItemData.city.isnot(None), ItemData.city != ""
        )

        return jsonify(paginate(query, page, limit))
    finally:
        db.close()


# ✅ Incomplete Items API with pagination
@item_bp.route("/incomplete", methods=["GET"])
def get_incomplete_items():
    db = SessionLocal()
    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 1000))

        query = db.query(ItemData).filter(
            or_(
                # name missing
                (ItemData.name.is_(None)) | (ItemData.name == ""),
                # all phones missing
                and_(
                    (ItemData.phone_no_1.is_(None)) | (ItemData.phone_no_1 == ""),
                    (ItemData.phone_no_2.is_(None)) | (ItemData.phone_no_2 == ""),
                    (ItemData.phone_no_3.is_(None)) | (ItemData.phone_no_3 == ""),
                    (ItemData.whatsapp_no.is_(None)) | (ItemData.whatsapp_no == ""),
                    (ItemData.virtual_phone_no.is_(None)) | (ItemData.virtual_phone_no == "")
                ),
                # category missing
                (ItemData.category.is_(None)) | (ItemData.category == ""),
                # sub category missing
                (ItemData.sub_category.is_(None)) | (ItemData.sub_category == ""),
                # area missing
                (ItemData.area.is_(None)) | (ItemData.area == ""),
                # city missing
                (ItemData.city.is_(None)) | (ItemData.city == "")
            )
        )

        return jsonify(paginate(query, page, limit))
    finally:
        db.close()
