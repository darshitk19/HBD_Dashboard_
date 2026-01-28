from flask import Blueprint, request, jsonify, current_app
from extensions import db
from model.scraper_task import ScraperTask
import threading

# Import the service function we created in Step 1
from services.scrapers.google_maps_service import run_google_maps_scraper

scraper_bp = Blueprint('scraper_bp', __name__)

@scraper_bp.route('/api/tasks', methods=['GET'])
def get_tasks():
    try:
        tasks = ScraperTask.query.order_by(ScraperTask.id.desc()).all()
        return jsonify([{
            "id": t.id,
            "platform": t.platform,
            "query": t.search_query,
            "status": t.status,
            "progress": t.progress,
            "errorMsg": t.error_message
        } for t in tasks]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@scraper_bp.route('/api/stop', methods=['POST'])
def stop_task():
    data = request.json
    task_id = data.get('task_id')
    task = ScraperTask.query.get(task_id)
    if task:
        task.should_stop = True
        db.session.commit()
        return jsonify({"message": "Stop signal sent"}), 200
    return jsonify({"error": "Task not found"}), 404

@scraper_bp.route('/api/tasks/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    task = ScraperTask.query.get(task_id)
    if task:
        db.session.delete(task)
        db.session.commit()
        return jsonify({"message": "Task deleted successfully"}), 200
    return jsonify({"error": "Task not found"}), 404

@scraper_bp.route('/api/scrape', methods=['POST'])
def start_deep_scrape():
    data = request.json
    category = data.get('category')
    city = data.get('city')
    platform = data.get('platform', 'Google Maps')

    # 1. Create Task in DB
    new_task = ScraperTask(
        platform=platform,
        search_query=f"{category} in {city}",
        location=city,
        status="starting"
    )
    db.session.add(new_task)
    db.session.commit()

    # 2. Start Scraper in Thread
    # Note: We pass `current_app._get_current_object()` so the thread can access the Flask context
    app = current_app._get_current_object()
    thread = threading.Thread(target=run_google_maps_scraper, args=(new_task.id, app))
    thread.start()

    return jsonify({"message": "Deep Scraper Started", "task_id": new_task.id}), 202

@scraper_bp.route('/api/results', methods=['GET'])
def api_results():
    # You can keep the raw SQL connection here if needed, 
    # OR import a service function to get results. 
    # For now, let's keep it simple and import the connection logic or move it to a service later.
    # (To save time, you can copy the 'api_results' function logic from your old app.py here)
    pass