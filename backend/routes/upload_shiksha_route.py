from flask import Blueprint,request,jsonify
from tasks.upload_shiksha_task import process_shiksha_task
from explains.utils import secure_filename  
import os

shiksha_bp = Blueprint('shiksha',__name__)

UPLOAD_DIR = "tmp/uploads"
os.makedirs(UPLOAD_DIR,exist_ok=True)

@shiksha_bp.route("/upload_shiksha_data",methods=["POST"])
def upload_shiksha_route():
    files = request.files.getlist("file")
    if not files:
        return jsonify({"error":"No files provided"}),400
    paths = []
    for f in files:
        file_name = secure_filename(f.filename)
        file_path = os.path.join(UPLOAD_DIR,file_name)
        f.save(file_path)
        paths.append(file_path)
    try:
        task = process_shiksha_task.delay(paths)
        return jsonify({
            "status":"files_accepted",
            "task_id":task.id
            }),202
    except Exception as e:
        return jsonify({
            "error":str(e)
        }),500