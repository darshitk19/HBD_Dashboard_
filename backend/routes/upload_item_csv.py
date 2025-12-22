# routes/item.py
from flask import Blueprint, request, jsonify
from sqlalchemy.orm import Session
import pandas as pd
from io import StringIO
from model.item_csv_model import ItemData  
from database.session import get_db_session

item_csv_bp = Blueprint("item", __name__)

@item_csv_bp.route("/upload_csv_item_data", methods=["POST"])
def upload_csv():
    """Upload CSV file and insert ItemData into DB"""
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files["file"]

    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    session: Session = get_db_session()

    try:
        # Read CSV contents
        contents = file.read().decode("utf-8")
        csv_data = StringIO(contents)

        # Load CSV into DataFrame
        df = pd.read_csv(csv_data)

        # Keep blank fields as empty string instead of NaN
        df = df.fillna("")

        # Convert numeric fields safely
        numeric_fields = ["ratings", "avg_spent", "cost_for_two", "pincode", "latitude", "longitude"]
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Insert rows into DB
        for _, row in df.iterrows():
            record = ItemData(
                category=row.get("category", ""),
                city=row.get("city", ""),
                name=row.get("name", ""),
                area=row.get("area", ""),
                address=row.get("address", ""),
                phone_no_1=row.get("phone_no_1", ""),
                phone_no_2=row.get("phone_no_2", ""),
                source=row.get("source", ""),
                ratings=row.get("ratings", 0),
                sub_category=row.get("sub_category", ""),
                state=row.get("state", ""),
                country=row.get("country", ""),
                email=row.get("email", ""),
                latitude=row.get("latitude", 0),
                longitude=row.get("longitude", 0),
                reviews=row.get("reviews", ""),
                facebook_url=row.get("facebook_url", ""),
                linkedin_url=row.get("linkedin_url", ""),
                twitter_url=row.get("twitter_url", ""),
                description=row.get("description", ""),
                pincode=row.get("pincode", 0),
                virtual_phone_no=row.get("virtual_phone_no", ""),
                whatsapp_no=row.get("whatsapp_no", ""),
                phone_no_3=row.get("phone_no_3", ""),
                avg_spent=row.get("avg_spent", 0),
                cost_for_two=row.get("cost_for_two", 0),
            )
            session.add(record)

        session.commit()
        return jsonify({"message": "CSV data uploaded successfully"}), 201

    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        session.close()
