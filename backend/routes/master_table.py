from flask import Blueprint, jsonify, request
from sqlalchemy import func, or_, text
from model.master_table_model import MasterTable
from database.session import get_db_session
from utils.storage import get_upload_base_dir
from werkzeug.utils import secure_filename
from tasks.upload_master_task import process_master_upload_task
from model.upload_master_reports_model import UploadReport

master_table_bp = Blueprint("master_table", __name__)

@master_table_bp.route("/upload/master", methods=["POST"])
def upload_master():
    files = request.files.getlist("file")
    if not files:
        return jsonify({"error": "No files provided"}), 400

    UPLOAD_DIR = get_upload_base_dir()/"master"
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    paths = []
    for f in files:
        filename = secure_filename(f.filename)
        path = UPLOAD_DIR/filename
        f.save(path)
        paths.append(str(path))

    task = process_master_upload_task.delay(paths)

    return jsonify({
        "status": "files_accepted",
        "task_id": task.id
    }), 202

@master_table_bp.route("/upload/report/<task_id>", methods=["GET"])
def get_upload_report(task_id):
    session = get_db_session()
    try:
        report = session.query(UploadReport).filter_by(task_id=task_id).first()

        if not report:
            return jsonify({
                "status": "not_found",
                "task_id": task_id
            }), 404

        base_stats = {
            "total_records": report.total_processed or 0,
            "total_cities": report.total_cities or 0,
            "total_areas": report.total_areas or 0,
            "total_categories": report.total_categories or 0,
            "city_match_status": (report.stats or {}).get(
                "city_match_status",
                {"matched": 0, "unmatched": 0}
            ),
            "missing_values": {
                "missing_phone": report.missing_primary_phone or 0,
                "missing_email": report.missing_email or 0,
                "missing_address": report.missing_address or 0
            }
        }

        return jsonify({
            "task_id": report.task_id,
            "status": report.status,
            "stats": base_stats
        })

    finally:
        session.close()

@master_table_bp.route("/master-dashboard-stats", methods=["GET"])
def get_master_dashboard_stats():
    session = get_db_session()
    task_id = request.args.get('task_id')
    
    where_clause = "WHERE 1=1"
    params = {}
    
    if task_id:
        where_clause += " AND task_id = :task_id"
        params['task_id'] = task_id

    try:
        # 1. Total Records
        total_query = text(f"SELECT COUNT(*) FROM master_table {where_clause}")
        total_records = session.execute(total_query, params).scalar()

        # 2. Distinct Counts
        counts_query = text(f"""
            SELECT 
                COUNT(DISTINCT city) as total_cities,
                COUNT(DISTINCT area) as total_areas,
                COUNT(DISTINCT category) as total_categories
            FROM master_table {where_clause}
        """)
        counts_result = session.execute(counts_query, params).fetchone()

        # 3. City Match Status
        match_status_query = text(f"""
            SELECT 
                SUM(CASE WHEN city IS NOT NULL AND city != '' THEN 1 ELSE 0 END) as matched,
                SUM(CASE WHEN city IS NULL OR city = '' THEN 1 ELSE 0 END) as unmatched
            FROM master_table {where_clause}
        """)
        match_status = session.execute(match_status_query, params).fetchone()

        # 4. Missing Values
        missing_query = text(f"""
            SELECT 
                SUM(CASE WHEN phone IS NULL OR phone = '' THEN 1 ELSE 0 END) as missing_phone,
                SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) as missing_email,
                SUM(CASE WHEN address IS NULL OR address = '' THEN 1 ELSE 0 END) as missing_address
            FROM master_table {where_clause}
        """)
        missing_stats = session.execute(missing_query, params).fetchone()

        # 5. Top 5 Cities
        city_counts_query = text(f"""
            SELECT city, COUNT(*) as count 
            FROM master_table {where_clause} 
            GROUP BY city 
            ORDER BY count DESC LIMIT 5
        """)
        city_counts = [dict(row._mapping) for row in session.execute(city_counts_query, params)]

        # 6. Top 5 Categories
        cat_counts_query = text(f"""
            SELECT category, COUNT(*) as count 
            FROM master_table {where_clause} 
            GROUP BY category 
            ORDER BY count DESC LIMIT 5
        """)
        category_counts = [dict(row._mapping) for row in session.execute(cat_counts_query, params)]

        # 7. Source Stats
        source_query = text(f"""
            SELECT source, COUNT(*) as count 
            FROM master_table {where_clause} 
            GROUP BY source 
            ORDER BY count DESC
        """)
        source_stats = [dict(row._mapping) for row in session.execute(source_query, params)]

        # 8. Top City + Category Combinations
        top_combo_query = text(f"""
            SELECT city, category, COUNT(*) as count
            FROM master_table {where_clause}
            GROUP BY city, category
            ORDER BY count DESC LIMIT 10
        """)
        top_city_categories = [dict(row._mapping) for row in session.execute(top_combo_query, params)]

        stats = {
            "total_records": total_records,
            "total_cities": counts_result.total_cities,
            "total_areas": counts_result.total_areas,
            "total_categories": counts_result.total_categories,
            "city_match_status": {
                "matched": int(match_status.matched or 0),
                "unmatched": int(match_status.unmatched or 0)
            },
            "missing_values": {
                "missing_phone": int(missing_stats.missing_phone or 0),
                "missing_email": int(missing_stats.missing_email or 0),
                "missing_address": int(missing_stats.missing_address or 0)
            },
            "city_counts": city_counts,
            "category_counts": category_counts,
            "source_stats": source_stats,
            "top_city_categories": top_city_categories
        }

        return jsonify({"status": "COMPLETED", "stats": stats})

    except Exception as e:
        print(f"Error fetching dashboard stats: {e}")
        return jsonify({"status": "ERROR", "message": str(e)}), 500
    finally:
        session.close()

@master_table_bp.route("/master_table/list", methods=["GET"])
def get_master_table_list():
    session = get_db_session()
    try:
        limit = request.args.get("limit", 10, type=int)
        cursor = request.args.get("cursor", type=int)

        limit = max(1, min(limit, 50))

        query = session.query(MasterTable).order_by(MasterTable.id.desc())
        if cursor:
            query = query.filter(MasterTable.id < cursor)

        rows = query.limit(limit + 1).all()

        has_next = len(rows) > limit
        rows = rows[:limit]

        data = [row.to_dict() for row in rows]
        next_cursor = data[-1]["id"] if has_next else None

        return jsonify({
            "limit": limit,
            "next_cursor": next_cursor,
            "has_next": has_next,
            "data": data
        })
    finally:
        session.close()