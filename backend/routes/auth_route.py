from flask import Blueprint, request, jsonify, current_app
from extensions import db, mail
from model.user import User
# IMPORT THESE NEW FUNCTIONS
from flask_jwt_extended import (
    create_access_token, 
    set_access_cookies, 
    unset_jwt_cookies, 
    get_jwt_identity
)
from utils.validators import is_valid_email, is_valid_password
from flask_mail import Message
import random
from datetime import datetime, timedelta

auth_bp = Blueprint("auth", __name__)

# ... (Signup remains the same) ...

# --- MODIFIED LOGIN ---
@auth_bp.route("/login", methods=["POST"])
def login():
    print("--- LOGIN ATTEMPT STARTED ---") 
    data = request.json or {}
    email = data.get("email", "").strip()
    password = data.get("password", "")

    if not email or not password:
        return jsonify({"message": "Email and password are required"}), 400

    user = User.query.filter_by(email=email).first()

    if not user:
        return jsonify({"message": "Invalid credentials"}), 401

    # Check Password (Direct comparison as per your previous request)
    if not user.check_password(password):
        return jsonify({"message": "Invalid credentials"}), 401

    print("--- SUCCESS: User verified. Setting Cookie. ---")
    
    # 1. Create the token
    token = create_access_token(identity=str(user.id))
    
    # 2. Create a JSON response
    response = jsonify({"message": "Login successful", "user_id": user.id})
    
    # 3. ATTACH THE COOKIE (This performs the "Remember Me" magic)
    set_access_cookies(response, token)
    
    return response, 200

# ... (Forgot Password / OTP routes remain the same) ...

# --- MODIFIED LOGOUT ---
@auth_bp.route("/logout", methods=["POST"])
def logout():
    response = jsonify({"message": "Logged out successfully"})
    # Delete the cookie from the browser
    unset_jwt_cookies(response)
    return response, 200