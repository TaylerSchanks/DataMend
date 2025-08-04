# DataMend

DataMend is a web-based tool built with Flask and JavaScript to assist Agvance users in uploading, validating, and processing customer data files before they are submitted to production systems.

![Agvance Logo](https://cdn2.assets-servd.host/agvance-preview/production/logos/logo-agvance.svg)

---

## 🚀 Features

- Test SQL Server connection
- View grower/customer tables from selected databases
- Run validations on uploaded files
- Modular backend for file-specific validations

---

## 🛠️ Technologies

- **Frontend**: HTML, CSS, JavaScript
- **Backend**: Python (Flask)
- **Database**: SQL Server (via `pyodbc`)

---

## 📁 Project Structure
DataMend/
├── backend/
│   ├── app.py                 # Flask API entry point
│   ├── requirements.txt       # Python dependencies
│   ├── run_commands.txt       # Dev notes/scripts
│   └── validators/            # Folder for validation modules
│       ├── __init__.py
│       └── validate_customer_base.py
├── frontend/
│   └── index.html             # Main UI
└── README.md



cd backend
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python app.py



