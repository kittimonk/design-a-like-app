
import sqlite3
import json
import logging
from typing import List, Dict, Any
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str = "data_mapping.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize database connection"""
        try:
            # Ensure the database file exists
            if not os.path.exists(self.db_path):
                open(self.db_path, 'a').close()
            logger.info(f"Database initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    def get_connection(self):
        """Get database connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # This enables column access by name
            return conn
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Create SourceTargetMapping table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS SourceTargetMapping (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_table TEXT NOT NULL,
                    source_column TEXT NOT NULL,
                    target_table TEXT NOT NULL,
                    target_column TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    session_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create RejectedRows table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS RejectedRows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    row_data TEXT NOT NULL,
                    rejection_reason TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    session_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create indexes for better performance
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_source_target_user 
                ON SourceTargetMapping(user_id, session_id)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_rejected_user 
                ON RejectedRows(user_id, session_id)
            ''')

            conn.commit()
            conn.close()
            logger.info("Database tables created successfully")

        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    def insert_source_target_mapping(self, mapping_data: Dict[str, Any]):
        """Insert approved mapping into SourceTargetMapping table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO SourceTargetMapping 
                (source_table, source_column, target_table, target_column, user_id, session_id)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                mapping_data['source_table'],
                mapping_data['source_column'],
                mapping_data['target_table'],
                mapping_data['target_column'],
                mapping_data['user_id'],
                mapping_data.get('session_id')
            ))

            conn.commit()
            mapping_id = cursor.lastrowid
            conn.close()
            
            logger.info(f"Inserted mapping with ID: {mapping_id}")
            return mapping_id

        except Exception as e:
            logger.error(f"Error inserting mapping: {e}")
            raise

    def insert_rejected_row(self, rejected_data: Dict[str, Any]):
        """Insert rejected row into RejectedRows table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO RejectedRows 
                (row_data, rejection_reason, user_id, session_id)
                VALUES (?, ?, ?, ?)
            ''', (
                json.dumps(rejected_data['row_data']),
                rejected_data['rejection_reason'],
                rejected_data['user_id'],
                rejected_data.get('session_id')
            ))

            conn.commit()
            rejected_id = cursor.lastrowid
            conn.close()
            
            logger.info(f"Inserted rejected row with ID: {rejected_id}")
            return rejected_id

        except Exception as e:
            logger.error(f"Error inserting rejected row: {e}")
            raise

    def get_approved_mappings(self, user_id: str, session_id: str = None) -> List[Dict[str, Any]]:
        """Get all approved mappings for a user"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            if session_id:
                cursor.execute('''
                    SELECT * FROM SourceTargetMapping 
                    WHERE user_id = ? AND session_id = ?
                    ORDER BY created_at DESC
                ''', (user_id, session_id))
            else:
                cursor.execute('''
                    SELECT * FROM SourceTargetMapping 
                    WHERE user_id = ?
                    ORDER BY created_at DESC
                ''', (user_id,))

            rows = cursor.fetchall()
            conn.close()

            # Convert to list of dictionaries
            mappings = []
            for row in rows:
                mappings.append({
                    'id': row['id'],
                    'source_table': row['source_table'],
                    'source_column': row['source_column'],
                    'target_table': row['target_table'],
                    'target_column': row['target_column'],
                    'user_id': row['user_id'],
                    'session_id': row['session_id'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                })

            logger.info(f"Retrieved {len(mappings)} mappings for user {user_id}")
            return mappings

        except Exception as e:
            logger.error(f"Error getting approved mappings: {e}")
            raise

    def get_rejected_rows(self, user_id: str, session_id: str = None) -> List[Dict[str, Any]]:
        """Get all rejected rows for a user"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            if session_id:
                cursor.execute('''
                    SELECT * FROM RejectedRows 
                    WHERE user_id = ? AND session_id = ?
                    ORDER BY created_at DESC
                ''', (user_id, session_id))
            else:
                cursor.execute('''
                    SELECT * FROM RejectedRows 
                    WHERE user_id = ?
                    ORDER BY created_at DESC
                ''', (user_id,))

            rows = cursor.fetchall()
            conn.close()

            # Convert to list of dictionaries
            rejected_rows = []
            for row in rows:
                rejected_rows.append({
                    'id': row['id'],
                    'row_data': json.loads(row['row_data']),
                    'rejection_reason': row['rejection_reason'],
                    'user_id': row['user_id'],
                    'session_id': row['session_id'],
                    'created_at': row['created_at']
                })

            logger.info(f"Retrieved {len(rejected_rows)} rejected rows for user {user_id}")
            return rejected_rows

        except Exception as e:
            logger.error(f"Error getting rejected rows: {e}")
            raise

    def clear_user_data(self, user_id: str, session_id: str = None):
        """Clear all data for a specific user (useful for testing)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            if session_id:
                cursor.execute('DELETE FROM SourceTargetMapping WHERE user_id = ? AND session_id = ?', (user_id, session_id))
                cursor.execute('DELETE FROM RejectedRows WHERE user_id = ? AND session_id = ?', (user_id, session_id))
            else:
                cursor.execute('DELETE FROM SourceTargetMapping WHERE user_id = ?', (user_id,))
                cursor.execute('DELETE FROM RejectedRows WHERE user_id = ?', (user_id,))

            conn.commit()
            conn.close()
            logger.info(f"Cleared data for user {user_id}")

        except Exception as e:
            logger.error(f"Error clearing user data: {e}")
            raise
