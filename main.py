
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import json
import logging
from typing import Dict, Any
from database import DatabaseManager
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Data Mapping API",
    description="API for data mapping and SQL generation",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database manager
db_manager = DatabaseManager()

@app.on_startup
async def startup_event():
    """Initialize database tables on startup"""
    try:
        db_manager.create_tables()
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")

@app.get("/")
async def root():
    return {"message": "Data Mapping API is running"}

@app.post("/compare-and-recommend")
async def compare_and_recommend(
    file: UploadFile = File(...),
    user_details: str = Form(...)
):
    """
    Process uploaded file and compare with existing data mappings
    """
    try:
        # Parse user details
        try:
            user_data = json.loads(user_details)
            user_id = user_data.get("user_id")
            session_id = user_data.get("session_id")
        except json.JSONDecodeError:
            raise HTTPException(status_code=422, detail="Invalid JSON format in user_details")

        if not user_id:
            raise HTTPException(status_code=422, detail="user_id is required in user_details")

        # Read the uploaded file
        contents = await file.read()
        
        # Determine file type and read accordingly
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        elif file.filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Please upload CSV or Excel files.")

        logger.info(f"Processing file: {file.filename} with {len(df)} rows")
        logger.info(f"User details: user_id={user_id}, session_id={session_id}")

        # Process the data
        approved_rows = []
        rejected_rows = []

        for index, row in df.iterrows():
            try:
                # Basic validation - check if required columns exist
                # Adjust these column names based on your actual data structure
                source_table = row.get('source_table', row.get('Source Table', ''))
                source_column = row.get('source_column', row.get('Source Column', ''))
                target_table = row.get('target_table', row.get('Target Table', ''))
                target_column = row.get('target_column', row.get('Target Column', ''))

                if source_table and source_column and target_table and target_column:
                    # Simple approval logic - you can enhance this with Azure OpenAI
                    mapping_data = {
                        'source_table': str(source_table),
                        'source_column': str(source_column),
                        'target_table': str(target_table),
                        'target_column': str(target_column),
                        'user_id': user_id,
                        'session_id': session_id
                    }
                    
                    # Insert approved mapping into database
                    db_manager.insert_source_target_mapping(mapping_data)
                    approved_rows.append(mapping_data)
                else:
                    # Reject row if missing required fields
                    rejection_reason = "Missing required fields: source_table, source_column, target_table, or target_column"
                    rejected_data = {
                        'row_data': row.to_dict(),
                        'rejection_reason': rejection_reason,
                        'user_id': user_id,
                        'session_id': session_id
                    }
                    
                    # Insert rejected row into database
                    db_manager.insert_rejected_row(rejected_data)
                    rejected_rows.append(rejected_data)

            except Exception as e:
                logger.error(f"Error processing row {index}: {e}")
                rejection_reason = f"Processing error: {str(e)}"
                rejected_data = {
                    'row_data': row.to_dict(),
                    'rejection_reason': rejection_reason,
                    'user_id': user_id,
                    'session_id': session_id
                }
                db_manager.insert_rejected_row(rejected_data)
                rejected_rows.append(rejected_data)

        message = f"Processed {len(df)} rows. Approved {len(approved_rows)} rows and rejected {len(rejected_rows)} rows."
        
        return JSONResponse({
            "message": message,
            "total_rows": len(df),
            "approved_count": len(approved_rows),
            "rejected_count": len(rejected_rows),
            "approved_rows": approved_rows,
            "rejected_rows": rejected_rows
        })

    except Exception as e:
        logger.error(f"Error in compare_and_recommend: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/generate-sql-logic")
async def generate_sql_logic(user_details: Dict[str, Any]):
    """
    Generate SQL SELECT query based on approved mappings
    """
    try:
        user_id = user_details.get("user_id")
        session_id = user_details.get("session_id")

        if not user_id:
            raise HTTPException(status_code=422, detail="user_id is required")

        # Get approved mappings from database
        mappings = db_manager.get_approved_mappings(user_id)

        if not mappings:
            return JSONResponse({
                "message": "No approved mapping data found for the user",
                "sql_query": None
            })

        # Generate SQL query based on mappings
        # Group by target tables
        target_tables = {}
        for mapping in mappings:
            target_table = mapping['target_table']
            if target_table not in target_tables:
                target_tables[target_table] = []
            target_tables[target_table].append(mapping)

        # Generate SELECT statements for each target table
        sql_queries = []
        for target_table, table_mappings in target_tables.items():
            columns = [mapping['target_column'] for mapping in table_mappings]
            sql_query = f"SELECT {', '.join(columns)} FROM {target_table};"
            sql_queries.append(sql_query)

        # Combine all queries
        final_sql = "\n".join(sql_queries)

        return JSONResponse({
            "message": f"Generated SQL query for {len(mappings)} approved mappings",
            "sql_query": final_sql,
            "mappings_count": len(mappings)
        })

    except Exception as e:
        logger.error(f"Error in generate_sql_logic: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "message": "API is running properly"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
