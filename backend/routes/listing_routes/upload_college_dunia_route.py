from flask import Flask,request,jsonify,Blueprint
from tasks.listings_task.upload_college_dunia_task import process_college_dunia_task
from werkzeug.utils import secure_filename
import os 
from utils.storage import get_upload_base_dir


college_dunia_bp = Blueprint('college_dunia_bp', __name__)
@college_dunia_bp.route('/upload/college-dunia-data', methods=['POST'])

def upload_college_dunia_data_route():
    files = request.files.getlist('files')
    if not files:
        return jsonify({"error": "No files provided"}), 400
    UPLOAD_DIR = get_upload_base_dir()/"college_dunia"
    UPLOAD_DIR.mkdir(parents=True,exist_ok=True)
    paths = []
    for file in files:
        filename = secure_filename(f.filename)
        filepath = UPLOAD_DIR/filename
        f.save(filepath)
        paths.append(str(filepath))
    try:
        task = process_college_dunia_task.delay(paths)
        return jsonify({"status":"files_accepted","task_id": task.id}), 202
    except Exception as e:
        return jsonify({"error": str(e)}), 500