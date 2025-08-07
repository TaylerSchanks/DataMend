from flask import Flask, request, jsonify, send_file, make_response
from flask_cors import CORS
import pyodbc
import pandas as pd
import tempfile
import os
import mimetypes
from flask import send_file

from validators.validate_customer_classifications import run_validation as validate_customer_classifications
from validators.validate_salesman import run_validation as validate_salesman

app = Flask(__name__)
CORS(app)

SQL_USERNAME = "Agvance"
SQL_PASSWORD = "AgvSQL2000"

def get_connection(server, database):
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD}"
    )

VALIDATION_MAP = {
    "Customer Classifications": validate_customer_classifications,
    "Salesman": validate_salesman
}

@app.route('/get-validation-options', methods=['GET'])
def get_validation_options():
    return jsonify(list(VALIDATION_MAP.keys()))

@app.route('/test-connection', methods=['POST'])
def test_connection():
    server = request.form.get('server')
    database = request.form.get('database')
    try:
        conn = get_connection(server, database)
        conn.close()
        return jsonify({"message": "✅ SQL Server connection successful!"})
    except Exception as e:
        return jsonify({"error": f"❌ Connection failed: {str(e)}"}), 500

@app.route('/get-growers', methods=['POST'])
def get_growers():
    server = request.form.get('server')
    database = request.form.get('database')
    try:
        conn = get_connection(server, database)
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 5 GROWID, GROWNAME1, GROWNAME2 FROM grower")
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        return jsonify({"data": rows})
    except Exception as e:
        return jsonify({"error": f"❌ Failed to retrieve grower table: {str(e)}"}), 500

@app.route('/process', methods=['POST'])
def process_file():
    server = request.form.get("server")
    database = request.form.get("database")
    data_type = request.form.get("dataType")
    uploaded_file = request.files.get("file")

    if not all([server, database, data_type]):
        return jsonify({"error": "❌ Missing required inputs."}), 400

    if data_type not in VALIDATION_MAP:
        return jsonify({"error": f"❌ Unknown data type: {data_type}"}), 400

    try:
        conn = get_connection(server, database)
        validator_func = VALIDATION_MAP[data_type]
        result = validator_func(conn, uploaded_file)
        conn.close()

        if result.get("file_type") == "sql":
            response_payload = {
                "message": result.get("message"),
                "download_link": result.get("download_link"),  # ✅ should match backend key
                "file_type": "sql"
            }
            print("✅ Returning SQL response:", response_payload)
            return jsonify(response_payload)

        elif result.get("file_type") == "excel":
            response_payload = {
                "message": result.get("message"),
                "download_link": result.get("download_link"),
                "file_type": "excel"
            }
            print("✅ Returning Excel response:", response_payload)
            return jsonify(response_payload)

        else:
            response_payload = {
                "message": result.get("message"),
                "file_path": None,
                "file_type": "none"
            }
            print("⚠️ Returning fallback response:", response_payload)
            return jsonify(response_payload)

    except Exception as e:
        return jsonify({"error": f"❌ Validation failed: {str(e)}"}), 500


    

@app.route('/download/tempfile')
def download_tempfile():
    filename = request.args.get('name')
    if not filename:
        return jsonify({"error": "❌ No file specified."}), 400

    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, filename)

    if os.path.exists(file_path):
        print(f"✅ Downloading file: {file_path}")

        # Determine MIME type or fall back
        mime_type = mimetypes.guess_type(file_path)[0] or 'application/octet-stream'

        # Force browser to download
        response = make_response(send_file(file_path))
        response.headers["Content-Type"] = mime_type
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response
    else:
        print(f"❌ File not found at: {file_path}")
        return jsonify({"error": "❌ File not found."}), 404


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
