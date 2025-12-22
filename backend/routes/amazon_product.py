from flask import Blueprint, jsonify
from sqlalchemy.orm import Session
from sqlalchemy import not_
from database.session import get_db_session
from model.amazon_product_model import AmazonProduct

amazon_products_bp = Blueprint("products", __name__)

# ✅ Helper: complete data filter
def is_complete_query(query):
    return query.filter(
        AmazonProduct.Product_name.isnot(None), AmazonProduct.Product_name != "",
        AmazonProduct.category.isnot(None), AmazonProduct.category != "",
        AmazonProduct.subcategory.isnot(None), AmazonProduct.subcategory != "",
        AmazonProduct.description.isnot(None), AmazonProduct.description != ""
    )


@amazon_products_bp.route("/products/complete", methods=["GET"])
def get_complete_products():
    session: Session = get_db_session()
    try:
        query = session.query(AmazonProduct)
        products = is_complete_query(query).all()

        # Convert SQLAlchemy objects → dicts
        result = [
            {column.name: getattr(p, column.name) for column in p.__table__.columns}
            for p in products
        ]
        return jsonify({"count": len(result), "data": result}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        session.close()


@amazon_products_bp.route("/products/incomplete", methods=["GET"])
def get_incomplete_products():
    session: Session = get_db_session()
    try:
        # First get complete product IDs
        complete_ids = [
            p.id for p in is_complete_query(session.query(AmazonProduct)).all()
        ]

        query = session.query(AmazonProduct)
        if complete_ids:
            products = query.filter(not_(AmazonProduct.id.in_(complete_ids))).all()
        else:
            products = query.all()

        # Convert SQLAlchemy objects → dicts
        result = [
            {column.name: getattr(p, column.name) for column in p.__table__.columns}
            for p in products
        ]
        return jsonify({"count": len(result), "data": result}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        session.close()
