
# Backend Setup Instructions

## Prerequisites
- Python 3.8 or higher
- pip (Python package installer)

## Setup Steps

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start the backend server:**
   ```bash
   python start_backend.py
   ```
   
   Or alternatively:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
   ```

3. **Access the API:**
   - API Base URL: `http://localhost:8000`
   - Swagger UI Documentation: `http://localhost:8000/docs`
   - Health Check: `http://localhost:8000/health`

## Frontend Setup

1. **Start the frontend (in a separate terminal):**
   ```bash
   npm run dev
   ```

2. **Access the frontend:**
   - Frontend URL: `http://localhost:5173` (or the port shown in terminal)

## Usage

1. Start both backend and frontend servers
2. Use the frontend UI to upload Excel/CSV files with mapping data
3. Enter user details in JSON format (e.g., `{"user_id": "abc123xy", "session_id": "session123"}`)
4. View the processed results in the Data Mapping Hub
5. Use the Test Data Generator to create SQL queries based on approved mappings

## File Format

Your Excel/CSV file should contain columns like:
- `source_table` or `Source Table`
- `source_column` or `Source Column`
- `target_table` or `Target Table`
- `target_column` or `Target Column`

## Database

The backend uses SQLite database (`data_mapping.db`) which will be created automatically.
