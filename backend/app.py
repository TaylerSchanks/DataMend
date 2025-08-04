from flask import Flask, request, jsonify
from flask_cors import CORS
import pyodbc
import pandas as pd
import tempfile
import os
from flask import send_file

from validators.validate_customer_classifications import run_validation as validate_customer_classifications


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
    "Customer Classifications": validate_customer_classifications
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
        return jsonify({"result": result})
    except Exception as e:
        return jsonify({"error": f"❌ Validation failed: {str(e)}"}), 500
    

@app.route('/download')
def download_file():
    path = request.args.get('path')
    if path and os.path.exists(path):
        return send_file(path, as_attachment=True)
    return jsonify({"error": "❌ File not found."}), 404


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
