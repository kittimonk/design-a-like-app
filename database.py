
import os
import pyodbc
from azure.identity import ManagedIdentityCredential, get_bearer_token_provider
from sqlalchemy import create_engine, text, Column, String, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.engine import URL
import pandas as pd
from datetime import import datetime

pyodbc.pooling = False

# Azure and database configuration
subscription_id = "d7da3-20-aaa4-b8bff"
client_id = "853-6a07d-acdc152"
object_id = "36-b2d6-4-4d8d39"

# Use Managed Identity Credential
msi = ManagedIdentityCredential(client_id=None if os.getenv("WEBSITE_INSTANCE_ID") else client_id)
azure_ad_token_provider = get_bearer_token_provider(msi, "https://cognitiveservices.azure.com/.default")
token = azure_ad_token_provider().encode('utf-8')

# SQL Server configuration
SQL_DRIVER = "{ODBC Driver 18 for SQL Server}"  # Change to "{ODBC Driver 17 for SQL Server}" if needed
SQL_SERVER = "003-eastus2-psql-7.database.windows.net,1433"
SQL_DATABASE = "ds-ai-ai-ent03-devdb"

# Connection string
connection_string = (
    f"DRIVER={SQL_DRIVER};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={object_id};"
    f"Authentication=ActiveDirectoryMsi;"
    f"Encrypt=Yes;"
)

# Create SQLAlchemy engine
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = create_engine(connection_url, pool_recycle=1500, pool_pre_ping=True, connect_args={"check_same_thread": False})

# SQLAlchemy session setup
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)
Base = declarative_base()

# Define database schema
class SourceTargetMapping(Base):
    __tablename__ = "source_target_mapping"
    __table_args__ = {"schema": "dbo"}

    id = Column(Integer, primary_key=True, autoincrement=True)  # Add surrogate key
    source_schema = Column(String(255), nullable=False)
    source_table = Column(String(255), nullable=False)
    source_columns = Column(String)
    source_data_types = Column(String)
    source_adls_location = Column(String)
    target_schema = Column(String)
    target_table = Column(String)
    target_columns = Column(String)
    target_data_types = Column(String)
    target_adls_location = Column(String)
    transformation_rule = Column(String)
    join_rule = Column(String)
    user_upload_details = Column(String)
    date_inserted = Column(DateTime, default=datetime.utcnow)
    date_updated = Column(DateTime)
    date_deleted = Column(DateTime)
    changes_applied = Column(String)
    user_upload_filename = Column(String)
    approved_by = Column(String)

class RejectedRows(Base):
    __tablename__ = "rejected_rows"
    __table_args__ = {"schema": "dbo"}

    id = Column(Integer, primary_key=True, autoincrement=True)  # Add surrogate key
    source_schema = Column(String)
    source_table = Column(String)
    source_columns = Column(String)
    source_data_types = Column(String)
    source_adls_location = Column(String)
    target_schema = Column(String)
    target_table = Column(String)
    target_columns = Column(String)
    target_data_types = Column(String)
    target_adls_location = Column(String)
    transformation_rule = Column(String)
    join_rule = Column(String)
    user_upload_details = Column(String)
    date_inserted = Column(DateTime, default=datetime.utcnow)
    date_updated = Column(DateTime)
    date_deleted = Column(DateTime)
    changes_applied = Column(String)
    user_upload_filename = Column(String(255))  # Remove primary key constraint
    rejected_by = Column(String)
    reject_reason = Column(String)

# Create tables if they don't exist
Base.metadata.create_all(engine)

def process_file(file_path, user_details):
    """
    Process an uploaded Excel file and insert data into the database.
    """
    session = Session()
    try:
        # Read the Excel file
        df = pd.read_excel(file_path, engine='openpyxl')

        # Check if the file has already been processed
        file_name = os.path.basename(file_path)
        existing_file = session.query(SourceTargetMapping).filter_by(user_upload_filename=file_name).first()
        if existing_file:
            raise ValueError(f"File '{file_name}' has already been processed.")

        # Process approved rows
        for _, row in df.iterrows():
            if row["approved"]:  # Assuming there's an "approved" column in the file
                mapping = SourceTargetMapping(
                    source_schema=row["source_schema"],
                    source_table=row["source_table"],
                    source_columns=row["source_columns"],
                    source_data_types=row["source_data_types"],
                    source_adls_location=row["source_adls_location"],
                    target_schema=row["target_schema"],
                    target_table=row["target_table"],
                    target_columns=row["target_columns"],
                    target_data_types=row["target_data_types"],
                    target_adls_location=row["target_adls_location"],
                    transformation_rule=row["transformation_rule"],
                    join_rule=row["join_rule"],
                    user_upload_details=user_details,
                    user_upload_filename=file_name,
                    approved_by=row["approved_by"]
                )
                session.add(mapping)
            else:
                # Log rejected rows
                rejected = RejectedRows(
                    source_schema=row["source_schema"],
                    source_table=row["source_table"],
                    source_columns=row["source_columns"],
                    source_data_types=row["source_data_types"],
                    source_adls_location=row["source_adls_location"],
                    target_schema=row["target_schema"],
                    target_table=row["target_table"],
                    target_columns=row["target_columns"],
                    target_data_types=row["target_data_types"],
                    target_adls_location=row["target_adls_location"],
                    transformation_rule=row["transformation_rule"],
                    join_rule=row["join_rule"],
                    user_upload_details=user_details,
                    user_upload_filename=file_name,
                    rejected_by=row["rejected_by"],
                    reject_reason=row["reject_reason"]
                )
                session.add(rejected)

        session.commit()
        print("File processed successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error processing file: {e}")
    finally:
        session.close()

# Test database connection
if __name__ == "__main__":
    session = Session()
    try:
        print("Testing database connection...")
        # Test a simple query to check the connection
        query = text("SELECT TOP 10 * FROM sys.tables")
        result = session.execute(query)
        # Query system tables to verify connection
        print("Connection successful! Retrieved data:")
        for row in result:
            print(row)
    except Exception as e:
        print(f"Database connection failed: {e}")
    finally:
        session.close()
