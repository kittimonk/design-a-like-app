
import os
import openai
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from azure.identity import ManagedIdentityCredential, get_bearer_token_provider
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine, text
from database import SourceTargetMapping, RejectedRows, engine
import logging
from datetime import datetime, timezone
import math, json
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("application.log"),
        logging.StreamHandler()
    ]
)

# Azure and database configuration
subscription_id = "d7da3-20-aaa4-b8bff"
client_id = "853-6a07d-acdc152"
object_id = "36-b2d6-4-4d8d39"
openai_resource_group_name = "003-eastus2-ai-openai-7"
openai_account_name = "003-eastus2-ai-openai-7"
openai_api_version = "2024-10-21"
openai_embedding_model = "text-embedding-3-small"
openai_lang_model = "gpt-4o-2024-05-13-tpm"  # Deployment name in Azure OpenAI

# Use Managed Identity for authentication
msi = ManagedIdentityCredential(client_id=None if os.getenv("WEBSITE_INSTANCE_ID") else client_id)

# Initialize OpenAI client
client = openai.AzureOpenAI(
    azure_endpoint=f"https://{openai_account_name}.openai.azure.com",
    api_version=openai_api_version,
    azure_ad_token_provider=get_bearer_token_provider(msi, "https://cognitiveservices.azure.com/.default")
)

# FastAPI App
app = FastAPI()

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for testing; restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# SQLAlchemy session setup
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

def replace_nan_with_none(data):
    """
    Recursively replace NaN values with None and convert datetime objects to SQL-compatible strings in a dictionary, list, or scalar.
    """
    if isinstance(data, dict):
        return {key: replace_nan_with_none(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [replace_nan_with_none(item) for item in data]
    elif isinstance(data, pd.Timestamp) or isinstance(data, datetime):
        return data.strftime('%Y-%m-%d %H:%M:%S')  # Convert datetime to SQL-compatible string
    elif isinstance(data, float) and math.isnan(data):
        return None
    else:
        return data

@app.post("/compare-and-recommend")
async def compare_and_recommend_endpoint(file: UploadFile = File(...), user: str = Form(...)):
    session = Session()
    try:
        logging.info(f"Received file upload request - filename: {file.filename}, user: {user}")
        
        # Validate file type
        if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
            raise HTTPException(status_code=400, detail="Invalid file type. Please upload Excel or CSV files only.")
        
        # Read file content
        file_content = await file.read()
        if not file_content:
            raise HTTPException(status_code=400, detail="Empty file uploaded.")
        
        # Save the uploaded file temporarily
        file_path = f"/tmp/{file.filename}"
        with open(file_path, "wb") as f:
            f.write(file_content)
        logging.info(f"File saved: {file_path}")

        # Read the file based on its type
        try:
            if file.filename.endswith('.csv'):
                df_uploaded = pd.read_csv(file_path)
            else:
                df_uploaded = pd.read_excel(file_path, engine='openpyxl')
        except Exception as e:
            logging.error(f"Error reading file: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")
        
        df_uploaded = df_uploaded.where(pd.notnull(df_uploaded), None)
        logging.info(f"Uploaded file read successfully. Rows: {len(df_uploaded)}")

        # Validate column order
        expected_columns = [
            "source_schema", "source_table", "source_columns", "source_data_types", "source_adls_location",
            "target_schema", "target_table", "target_columns", "target_data_types", "target_adls_location",
            "transformation_rule", "join_rule"
        ]
        if set(df_uploaded.columns) != set(expected_columns):
            logging.error("Invalid column order in uploaded file.")
            logging.error(f"Expected columns: {expected_columns}")
            logging.error(f"Actual columns: {list(df_uploaded.columns)}")
            raise HTTPException(status_code=400, detail="Invalid Excel format. Ensure the file has the correct columns.")

        # Check for duplicate file processing
        existing_files = session.query(SourceTargetMapping.user_upload_filename).filter_by(user_upload_filename=file.filename).distinct().all()
        if file.filename in [file_record[0] for file_record in existing_files]:
            logging.error(f"Duplicate file detected: {file.filename}")
            raise HTTPException(status_code=400, detail=f"The file '{file.filename}' has already been processed.")

        # Fetch existing data
        existing_data = session.query(SourceTargetMapping).all()
        df_existing = pd.DataFrame([row.__dict__ for row in existing_data])
        if "_sa_instance_state" in df_existing.columns:
            df_existing = df_existing.drop("_sa_instance_state", axis=1)
        logging.info(f"Fetched existing data from database. Rows: {len(df_existing)}")

        # Split data into batches
        batch_size = 10
        batches = [df_uploaded[i:i + batch_size] for i in range(0, len(df_uploaded), batch_size)]
        approved_rows = []
        rejected_rows = []

        for batch_index, batch in enumerate(batches):
            try:
                logging.info(f"Processing batch {batch_index + 1}/{len(batches)} with {len(batch)} rows.")
                prompt = f"""
You are a data engineer assistant. Compare the following uploaded data with the existing approved data in the database.
Recommend whether each row in the uploaded data should be approved or rejected. If rejected, provide the detailed reason.

Validation Criteria:
- Reject rows with invalid 'join_rule' syntax or logic (but not if 'join_rule' is missing).
- Reject rows with invalid 'transformation_rule' syntax or logic.
- Do not reject rows for mismatched data types between source and target columns.

Return the response as a valid JSON array where each object contains:
- "status": "approved" or "rejected"
- "reason": A detailed explanation for rejection (if applicable).
- "row_index": The index of the row being processed.

Uploaded Data (Batch Size: {len(batch)}):
{batch.to_dict(orient='records')}

Existing Data (Sample Size: {min(len(df_existing), 50)}):
{df_existing.head(50).to_dict(orient='records')}
"""
                
                logging.info(f"Prompt sent to OpenAI: {prompt}")

                retry_count = 0
                max_retries = 5
                recommendations = None
                while retry_count < max_retries:
                    try:
                        response = client.chat.completions.create(
                            model=openai_lang_model,
                            messages=[
                                {"role": "system", "content": "You are a helpful data engineer assistant."},
                                {"role": "user", "content": prompt}
                            ],
                            max_tokens=1000,
                            temperature=0.0
                        )
                        recommendations = response.choices[0].message.content.strip()
                        if recommendations:
                            break
                    except openai.RateLimitError as e:
                        logging.error(f"Rate limit exceeded: {str(e)}, Retrying...")
                        retry_count += 1
                        time.sleep(min(2 ** retry_count, 60))
                    except Exception as e:
                        logging.error(f"Unexpected error: {str(e)}")
                        break

                if not recommendations:
                    logging.error("OpenAI response is empty.")
                    continue

                # Log raw OpenAI response
                logging.info(f"Raw OpenAI response: {recommendations}")

                # Extract JSON array from response
                try:
                    start_index = recommendations.find("[")
                    end_index = recommendations.rfind("]") + 1
                    if start_index == -1 or end_index == -1:
                        raise ValueError("Response does not contain a valid JSON array.")
                    
                    json_array = recommendations[start_index:end_index]
                    recommendations_data = json.loads(json_array)
                    logging.info(f"Parsed recommendations: {recommendations_data}")
                except Exception as e:
                    logging.error(f"Error parsing OpenAI response: {str(e)}")
                    continue

                # Process rows based on recommendations
                for recommendation in recommendations_data:
                    row_index = recommendation.get("row_index")
                    if row_index is None or row_index >= len(batch):
                        logging.warning(f"Invalid row_index in recommendation: {recommendation}")
                        continue

                    row = batch.iloc[row_index].to_dict()
                    row["user_upload_details"] = user
                    row["date_inserted"] = datetime.now(timezone.utc)
                    row["date_updated"] = datetime.now(timezone.utc)
                    row["date_deleted"] = None
                    row["user_upload_filename"] = file.filename

                    if recommendation.get("status") == "approved":
                        row["changes_applied"] = "Row approved and inserted by OpenAI Assistant"
                        row["approved_by"] = "OpenAI Assistant"
                        approved_rows.append(row)
                    elif recommendation.get("status") == "rejected":
                        row["changes_applied"] = "Row rejected by OpenAI Assistant"
                        row["rejected_by"] = "OpenAI Assistant"
                        row["reject_reason"] = recommendation.get("reason", "Reason provided by OpenAI")
                        rejected_rows.append(row)
                    else:
                        logging.warning(f"Unexpected status in recommendation: {recommendation}")

            except Exception as e:
                logging.error(f"Error processing batch {batch_index + 1}: {str(e)}")

        # Replace NaN values with None before inserting into the database
        approved_rows = replace_nan_with_none(approved_rows)
        rejected_rows = replace_nan_with_none(rejected_rows)

        logging.info(f"Cleaned approved rows: {approved_rows}")
        logging.info(f"Cleaned rejected rows: {rejected_rows}")

        # Insert rejected rows into the database first
        try:
            for row in rejected_rows:
                session.add(RejectedRows(**row))
            session.commit()
            logging.info(f"Inserted {len(rejected_rows)} rejected rows successfully")
        except Exception as e:
            logging.error(f"Error inserting rejected rows: {str(e)}")
            session.rollback()

        # Insert approved rows into the database
        try:
            for row in approved_rows:
                session.add(SourceTargetMapping(**row))
            session.commit()
            logging.info(f"Inserted {len(approved_rows)} approved rows successfully")
        except Exception as e:
            logging.error(f"Error inserting approved rows: {str(e)}")
            session.rollback()

        return {
            "message": f"Processed {len(df_uploaded)} rows. Approved {len(approved_rows)} rows and rejected {len(rejected_rows)} rows.",
            "approved_rows": approved_rows,
            "rejected_rows": rejected_rows,
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in compare-and-recommend: {str(e)}")
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/generate-sql-logic")
async def generate_sql_logic_endpoint(source_table: str, target_table: str):
    """
    Endpoint to generate SQL SELECT query logic based on transformation and join rules.
    """
    session = Session()
    try:
        # Log input parameters
        logging.info(f"Received request for source_table: {source_table}, target_table: {target_table}")

        # Query the database for column mappings and transformation rules
        query = text("""
        SELECT source_columns, target_columns, transformation_rule, join_rule
        FROM dbo.source_target_mapping
        WHERE LOWER(source_table) = LOWER(:source_table) AND LOWER(target_table) = LOWER(:target_table)
        """)
        mappings = session.execute(query, {"source_table": source_table, "target_table": target_table}).fetchall()

        if not mappings:
            raise HTTPException(
                status_code=404, 
                detail=f"No column mappings or transformation rules found for source table '{source_table}' and target table '{target_table}'."
            )

        # Format the mappings for SQL query construction
        select_clauses = []
        join_clause = None

        for mapping in mappings:
            source_column = mapping[0]
            target_column = mapping[1]
            transformation_rule = mapping[2]
            join_rule = mapping[3]

            # Apply transformation rule if present
            if transformation_rule and transformation_rule.lower() != "straight move":
                select_clauses.append(f"{transformation_rule} AS {target_column}")
            else:
                select_clauses.append(f"id.{source_column} AS {target_column}")

            # Capture join rule if present
            if join_rule and not join_clause:
                join_clause = join_rule

        # Construct the SELECT query
        select_clause = ", ".join(select_clauses)
        base_query = f"SELECT DISTINCT {select_clause} FROM {source_table} id"

        # Add JOIN clause if present
        if join_clause:
            base_query += f" LEFT JOIN {join_clause}"

        # Format the SQL query to remove line breaks
        formatted_query = base_query.replace('\n', ' ').strip()

        # Return the formatted SQL query directly
        return {"sql_logic": formatted_query}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in generate-sql-logic: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "Backend is running successfully"}

# Serve static files from the 'dist' folder
static_path = os.path.join(os.path.dirname(__file__), 'dist')
if os.path.exists(static_path):
    app.mount("/", StaticFiles(directory=static_path, html=True), name="static")

# Main block to run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
