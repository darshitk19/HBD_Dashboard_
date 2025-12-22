from flask import Blueprint, request, jsonify
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, Text, MetaData, inspect
from sqlalchemy.exc import SQLAlchemyError
import os
from flask_cors import CORS
import chardet  # <-- added for automatic encoding detection

# Blueprint
upload_others_csv_bp = Blueprint("upload_others_csv", __name__)
CORS(upload_others_csv_bp)

# SQLAlchemy engine
engine = create_engine("mysql+pymysql://root:vivek123@localhost:3306/dummy")
metadata = MetaData()

@upload_others_csv_bp.route("/upload_others_csv", methods=["POST"])
def upload_csv():
    try:
        # File receive check
        if "file" not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        file = request.files["file"]
        if file.filename == "":
            return jsonify({"error": "Empty filename"}), 400

        # Table name (safe)
        filename = os.path.splitext(file.filename)[0].strip().replace(" ", "_").lower()

        # ---- Detect encoding ----
        rawdata = file.read()
        result = chardet.detect(rawdata)
        encoding = result.get("encoding", "utf-8") or "utf-8"
        print(f"Detected encoding: {encoding}")

        # reset file pointer after reading
        file.seek(0)

        # ---- Read CSV with detected encoding ----
        try:
            df = pd.read_csv(file, encoding=encoding, on_bad_lines="skip")
        except UnicodeDecodeError:
            # fallback to latin1 if utf-8 or detected encoding fails
            file.seek(0)
            df = pd.read_csv(file, encoding="latin1", on_bad_lines="skip")

        if df.empty:
            return jsonify({"error": "CSV is empty or invalid"}), 400

        # Table existence check
        inspector = inspect(engine)
        if inspector.has_table(filename):
            return jsonify({
                "message": f"Table '{filename}' already exists.",
                "filename": filename,
                "columns": list(df.columns),
                "rows_inserted": 0
            }), 200

        # Convert all data to string (safe for large unknown data)
        df = df.fillna("").astype(str)

        # Columns prepare (all Text type for safety)
        columns = [
            Column(
                str(col).strip().replace(" ", "_").replace("-", "_").lower(),
                Text()
            )
            for col in df.columns
        ]

        # Create table with id
        table = Table(
            filename,
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            *columns
        )
        metadata.create_all(engine)

        # Insert data in chunks
        chunksize = 5000  # adjust based on memory
        rows_inserted = 0
        for start in range(0, len(df), chunksize):
            df.iloc[start:start+chunksize].to_sql(filename, engine, if_exists="append", index=False)
            rows_inserted += len(df.iloc[start:start+chunksize])

        return jsonify({
            "message": f"Table '{filename}' created successfully.",
            "filename": filename,
            "columns": list(df.columns),
            "rows_inserted": rows_inserted
        })

    except SQLAlchemyError as e:
        print("SQLAlchemy Error:", str(e))
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        print("CSV Upload Error:", str(e))
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
