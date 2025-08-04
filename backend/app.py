from flask import Flask, request, jsonify
from flask_cors import CORS
import pyodbc

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
        cursor.execute("SELECT TOP 5 GROWID, GROWNAME1, GROWNAME2 FROM grower")  # limit for now
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        return jsonify({"data": rows})
    except Exception as e:
        return jsonify({"error": f"❌ Failed to retrieve grower table: {str(e)}"}), 500

@app.route('/process', methods=['POST'])
def process_file():
    # Placeholder for file upload + validation
    return jsonify({"error": "Validation not yet implemented."}), 501

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
