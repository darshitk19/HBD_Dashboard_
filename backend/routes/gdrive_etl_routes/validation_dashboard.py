"""
📊 Validation Dashboard API Routes
🔍 Provides endpoints for the ETL validation pipeline dashboard.
🌐 Supports ALL Indian languages — Zero data loss
"""
from flask import Blueprint, jsonify, request
from sqlalchemy import text
from database.session import engine
import logging

logger = logging.getLogger("ValidationDashboard")
validation_dashboard_bp = Blueprint("validation_dashboard", __name__)

# 🗄️ Table Names
VALIDATION_TABLE = "validation_raw_google_map"
CLEAN_TABLE = "raw_clean_google_map_data"
RAW_TABLE = "raw_google_map_drive_data"


def execute_read(query, params=None):
    """⚡ Fast read query executor with READ UNCOMMITTED for speed."""
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
        result = conn.execute(text(query), params or {})
        return result.fetchall()


# ═══════════════════════════════════════════════════════════════════
# 📊 MAIN DASHBOARD — All stats in one call
# ═══════════════════════════════════════════════════════════════════

@validation_dashboard_bp.route("/api/validation/dashboard", methods=["GET"])
def get_validation_dashboard():
    """📊 Main dashboard endpoint — returns all stats in one call."""
    try:
        # 📈 1. Overall counts
        raw_count = execute_read(f"SELECT COUNT(*) FROM {RAW_TABLE}")[0][0]
        validation_count = execute_read(f"SELECT COUNT(*) FROM {VALIDATION_TABLE}")[0][0]
        clean_count = execute_read(f"SELECT COUNT(*) FROM {CLEAN_TABLE}")[0][0]

        # 🏷️ 2. Validation status breakdown
        status_rows = execute_read(
            f"SELECT validation_status, COUNT(*) FROM {VALIDATION_TABLE} GROUP BY validation_status"
        )
        validation_breakdown = {r[0]: r[1] for r in status_rows}

        # 🧹 3. Cleaning status breakdown
        cleaning_rows = execute_read(
            f"SELECT cleaning_status, COUNT(*) FROM {VALIDATION_TABLE} GROUP BY cleaning_status"
        )
        cleaning_breakdown = {r[0]: r[1] for r in cleaning_rows}

        # 📊 4. Pipeline progress percentage
        pending = validation_breakdown.get("PENDING", 0)
        structured = validation_breakdown.get("STRUCTURED", 0)
        invalid = validation_breakdown.get("INVALID", 0)
        missing = validation_breakdown.get("MISSING", 0)
        duplicate = validation_breakdown.get("DUPLICATE", 0)
        cleaned = cleaning_breakdown.get("CLEANED", 0)
        not_started = cleaning_breakdown.get("NOT_STARTED", 0)

        ingestion_pct = round((validation_count / raw_count * 100), 2) if raw_count > 0 else 0
        validation_pct = round(((validation_count - pending) / validation_count * 100), 2) if validation_count > 0 else 0
        cleaning_pct = round((cleaned / validation_count * 100), 2) if validation_count > 0 else 0

        # ❌ 5. Error rows (most recent 100)
        error_rows = execute_read(f"""
            SELECT id, raw_id, name, city, state, category, phone_number,
                   validation_status, missing_fields, invalid_format_fields, duplicate_reason, processed_at
            FROM {VALIDATION_TABLE}
            WHERE validation_status IN ('INVALID', 'MISSING', 'DUPLICATE')
            ORDER BY processed_at DESC
            LIMIT 100
        """)
        errors = []
        for r in error_rows:
            errors.append({
                "id": r[0], "raw_id": r[1], "name": r[2], "city": r[3], "state": r[4],
                "category": r[5], "phone_number": r[6], "validation_status": r[7],
                "missing_fields": r[8], "invalid_format_fields": r[9],
                "duplicate_reason": r[10], "processed_at": str(r[11]) if r[11] else None
            })

        # ✅ 6. Clean data sample (most recent 50)
        clean_rows = execute_read(f"""
            SELECT id, raw_id, name, address, phone_number, category, city, state, reviews_count, reviews_avg, created_at
            FROM {CLEAN_TABLE}
            ORDER BY id DESC
            LIMIT 50
        """)
        clean_data = []
        for r in clean_rows:
            clean_data.append({
                "id": r[0], "raw_id": r[1], "name": r[2], "address": r[3],
                "phone_number": r[4], "category": r[5], "city": r[6], "state": r[7],
                "reviews_count": r[8], "reviews_avg": float(r[9] or 0), "created_at": str(r[10]) if r[10] else None
            })

        return jsonify({
            "status": "success",
            "raw_count": raw_count,
            "validation_count": validation_count,
            "clean_count": clean_count,
            "validation_breakdown": validation_breakdown,
            "cleaning_breakdown": cleaning_breakdown,
            "progress": {
                "ingestion_pct": ingestion_pct,
                "validation_pct": validation_pct,
                "cleaning_pct": cleaning_pct
            },
            "summary": {
                "pending": pending,
                "structured": structured,
                "invalid": invalid,
                "missing": missing,
                "duplicate": duplicate,
                "cleaned": cleaned,
                "not_started": not_started
            },
            "errors": errors,
            "clean_data": clean_data
        })

    except Exception as e:
        logger.error(f"🔥 Validation Dashboard Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════
# ❌ ERRORS — Paginated error rows
# ═══════════════════════════════════════════════════════════════════

@validation_dashboard_bp.route("/api/validation/errors", methods=["GET"])
def get_validation_errors():
    """❌ Paginated error rows — INVALID, MISSING, DUPLICATE."""
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 50))
    status_filter = request.args.get("status")  # 🏷️ INVALID, MISSING, DUPLICATE
    field_filter = request.args.get("field")      # 🔍 name, address, etc.
    offset = (page - 1) * limit

    params = {"limit": limit, "offset": offset}
    where_clauses = ["validation_status IN ('INVALID', 'MISSING', 'DUPLICATE')"]

    if status_filter:
        where_clauses[0] = "validation_status = :status"
        params["status"] = status_filter
    
    if field_filter:
        # 🔍 Use FIND_IN_SET to match field in comma-separated missing_fields column
        where_clauses.append("FIND_IN_SET(:field, missing_fields)")
        params["field"] = field_filter

    where = f"WHERE {' AND '.join(where_clauses)}"

    try:
        rows = execute_read(f"""
            SELECT id, raw_id, name, city, state, category, phone_number,
                   validation_status, missing_fields, invalid_format_fields, duplicate_reason, processed_at
            FROM {VALIDATION_TABLE}
            {where}
            ORDER BY processed_at DESC
            LIMIT :limit OFFSET :offset
        """, params)

        data = []
        for r in rows:
            data.append({
                "id": r[0], "raw_id": r[1], "name": r[2], "city": r[3], "state": r[4],
                "category": r[5], "phone_number": r[6], "validation_status": r[7],
                "missing_fields": r[8], "invalid_format_fields": r[9],
                "duplicate_reason": r[10], "processed_at": str(r[11]) if r[11] else None
            })

        # 📊 Total count for pagination
        total = execute_read(f"SELECT COUNT(*) FROM {VALIDATION_TABLE} {where}", params)[0][0]

        return jsonify({"status": "success", "data": data, "page": page, "total": total})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════
# ✅ CLEAN DATA — Paginated production data
# ═══════════════════════════════════════════════════════════════════

@validation_dashboard_bp.route("/api/validation/clean", methods=["GET"])
def get_clean_data():
    """✅ Paginated clean/production data."""
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 50))
    offset = (page - 1) * limit

    try:
        rows = execute_read(f"""
            SELECT id, raw_id, name, address, website, phone_number, toll_free_number, reviews_count, reviews_avg,
                   category, subcategory, city, state, area, created_at
            FROM {CLEAN_TABLE}
            ORDER BY id DESC
            LIMIT :limit OFFSET :offset
        """, {"limit": limit, "offset": offset})

        data = []
        for r in rows:
            data.append({
                "id": r[0], "raw_id": r[1], "name": r[2], "address": r[3], "website": r[4],
                "phone_number": r[5], "toll_free_number": r[6], "reviews_count": r[7], "reviews_avg": float(r[8] or 0),
                "category": r[9], "subcategory": r[10], "city": r[11], "state": r[12],
                "area": r[13], "created_at": str(r[14]) if r[14] else None
            })

        total = execute_read(f"SELECT COUNT(*) FROM {CLEAN_TABLE}")[0][0]
        return jsonify({"status": "success", "data": data, "page": page, "total": total})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════
# 📈 VALIDATION REPORT — Detailed analytics
# ═══════════════════════════════════════════════════════════════════

@validation_dashboard_bp.route("/api/validation/report", methods=["GET"])
def get_validation_report():
    """📈 Detailed analytics for the validation report dashboard."""
    batch_id = request.args.get("batch_id")
    start_date = request.args.get("start_date")  # 📅 YYYY-MM-DD
    end_date = request.args.get("end_date")      # 📅 YYYY-MM-DD

    where_clauses = ["validation_status != 'PENDING'"]
    params = {}

    if batch_id:
        # 🔖 Note: If batching logic added later, we can filter here
        pass
    if start_date:
        where_clauses.append("processed_at >= :start")
        params["start"] = f"{start_date} 00:00:00"
    if end_date:
        where_clauses.append("processed_at <= :end")
        params["end"] = f"{end_date} 23:59:59"

    where_str = f"WHERE {' AND '.join(where_clauses)}"

    try:
        # 🎯 1. KPIs
        rows = execute_read(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN validation_status = 'STRUCTURED' THEN 1 ELSE 0 END) as valid,
                SUM(CASE WHEN validation_status IN ('INVALID', 'MISSING') THEN 1 ELSE 0 END) as invalid,
                MAX(processed_at) as last_updated
            FROM {VALIDATION_TABLE}
            {where_str}
        """, params)
        
        total, valid, invalid, last_updated = rows[0]
        total = int(total or 0)
        valid = int(valid or 0)
        invalid = int(invalid or 0)
        accuracy = round((valid / total * 100), 2) if total > 0 else 0

        # 🔍 2. Missing Fields Analytics
        # 📋 Fields to check: name, address, category, city, state, phone_number
        fields = ["name", "address", "category", "city", "state", "phone_number"]
        missing_stats = []
        for field in fields:
            # ✅ Only NULL or TRIM empty — NOT language-based rejection
            m_count = execute_read(f"""
                SELECT COUNT(*) FROM {VALIDATION_TABLE}
                {where_str} AND ({field} IS NULL OR TRIM({field}) = '')
            """, params)[0][0]
            
            missing_stats.append({
                "field": field.replace('_', ' ').capitalize() if field != 'phone_number' else 'Contact',
                "raw_field": field,
                "count": int(m_count),
                "percentage": round((m_count / total * 100), 2) if total > 0 else 0
            })

        # 📈 3. 7-Day Trend
        trend_rows = execute_read(f"""
            SELECT DATE(processed_at) as d, COUNT(*) 
            FROM {VALIDATION_TABLE}
            WHERE processed_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY d ORDER BY d ASC
        """)
        trend = [{"date": str(r[0]), "count": int(r[1])} for r in trend_rows]

        report_data = {
            "status": "success",
            "kpis": {
                "total": total,
                "valid": valid,
                "invalid": invalid,
                "accuracy": accuracy,
                "last_updated": str(last_updated) if last_updated else None
            },
            "missing_report": missing_stats,
            "trend": trend,
            "state_stats": []
        }

        # 🗺️ 4. State-wise Breakdown (Quality per state)
        state_rows = execute_read(f"""
            SELECT 
                state,
                COUNT(*) as total,
                SUM(CASE WHEN validation_status = 'STRUCTURED' THEN 1 ELSE 0 END) as valid
            FROM {VALIDATION_TABLE}
            {where_str}
            GROUP BY state
            ORDER BY total DESC
            LIMIT 15
        """, params)
        
        for r in state_rows:
            s_total = int(r[1])
            s_valid = int(r[2])
            report_data["state_stats"].append({
                "state": r[0] if r[0] else "Unknown",
                "total": s_total,
                "accuracy": round((s_valid / s_total * 100), 2) if s_total > 0 else 0
            })

        return jsonify(report_data)
    except Exception as e:
        logger.error(f"🔥 Report API error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
