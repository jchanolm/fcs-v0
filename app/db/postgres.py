# /app/db/postgres.py
"""
PostgreSQL connection and utility functions using SQLAlchemy.
"""
import logging
import time
from typing import List, Dict, Any, Optional, Generator
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
import os
from app.config import POSTGRES_CONNECTION_STRING

from dotenv import load_dotenv
load_dotenv(override=True)

# Set up logging
logger = logging.getLogger(__name__)

# Global SQL utils instance
sql_utils = None

def init_postgres():
    """Initialize PostgreSQL connection using SQLUtils."""
    global sql_utils
    
    try:
        if not POSTGRES_CONNECTION_STRING:
            logger.warning("POSTGRES_CONNECTION_STRING not found in environment variables")
            return False
            
        logger.info("Initializing PostgreSQL connection with SQLUtils")
        
        sql_utils = SQLUtils(connection_string=POSTGRES_CONNECTION_STRING)
        
        # Test the connection
        test_result = sql_utils.execute_query("SELECT version() as version")
        if test_result:
            logger.info(f"Connected to PostgreSQL: {test_result[0]['version']}")
            return True
        else:
            logger.error("Failed to test PostgreSQL connection")
            return False
        
    except Exception as e:
        logger.error(f"PostgreSQL connection error: {str(e)}")
        sql_utils = None
        return False

def execute_postgres_query(
    query: str, 
    params: Optional[Dict[str, Any]] = None,
    fetch_size: int = 10000
) -> List[Dict[str, Any]]:
    """
    Execute a PostgreSQL query and return results as a list of dictionaries.
    
    Args:
        query: SQL query string with named parameters (e.g., :param_name)
        params: Query parameters dictionary
        fetch_size: Number of rows to fetch at a time
        
    Returns:
        List[Dict[str, Any]]: Query results as list of dicts
    """
    if sql_utils is None:
        logger.error("PostgreSQL connection not initialized")
        return []
        
    try:
        return sql_utils.execute_query(query, params, fetch_size)
    except Exception as e:
        logger.error(f"PostgreSQL query execution error: {str(e)}")
        return []

def close_postgres_connection():
    """Close the PostgreSQL connection."""
    global sql_utils
    if sql_utils is not None:
        # SQLAlchemy engine will handle cleanup automatically
        sql_utils = None
        logger.info("PostgreSQL connection closed")


class SQLUtils:
    """Helper class for SQL database operations with connection pooling and retry logic."""
    
    def __init__(self, connection_string: Optional[str] = None, max_retries: int = 5):
        """
        Initialize SQL utilities with connection pooling.
        
        Args:
            connection_string: SQLAlchemy connection string. If None, uses SQL_CONNECTION_STRING env var
            max_retries: Maximum number of retry attempts for failed queries
        """
        self.connection_string = connection_string or os.environ.get('SQL_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("No connection string provided")
            
        self.max_retries = max_retries
        
        # Create engine with connection pooling
        self.engine = create_engine(
            self.connection_string,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=3600  # Recycle connections after 1 hour
        )
        
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()
            
    def execute_query(self, 
                    query: str, 
                    params: Optional[Dict[str, Any]] = None,
                    fetch_size: int = 10000,
                    counter: int = 0) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch_size: Number of rows to fetch at a time
            counter: Retry counter
            
        Returns:
            List[Dict[str, Any]]: Query results as list of dicts
        """
        time.sleep(counter * 2)  # Exponential backoff
        
        if counter > self.max_retries:
            logging.error(f"Max retries ({self.max_retries}) exceeded for query")
            raise Exception(f"Query failed after {self.max_retries} attempts")
            
        try:
            with self.get_connection() as conn:
                # Execute query directly with SQLAlchemy
                result = conn.execute(text(query), params or {})
                
                # Convert rows to list of dicts
                rows = []
                for row in result:
                    rows.append(dict(row._mapping))
                    
                return rows
                
        except Exception as e:
            logging.error(f"Query execution failed: {e}")
            logging.info(f"Retrying... (attempt {counter + 1}/{self.max_retries})")
            return self.execute_query(query, params, fetch_size, counter + 1)

    def execute_query_df(self, 
                        query: str, 
                        params: Optional[Dict[str, Any]] = None,
                        fetch_size: int = 10000,
                        counter: int = 0) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch_size: Number of rows to fetch at a time
            counter: Retry counter
            
        Returns:
            pd.DataFrame: Query results
        """
        return self.execute_query(query, params, fetch_size, counter)
            
    def execute_query_streaming(self, 
                               query: str, 
                               params: Optional[Dict[str, Any]] = None,
                               chunk_size: int = 10000) -> Generator[pd.DataFrame, None, None]:
        """
        Execute query and yield results in chunks for memory efficiency.
        
        Args:
            query: SQL query string
            params: Query parameters
            chunk_size: Number of rows per chunk
            
        Yields:
            pd.DataFrame: Chunks of query results
        """
        try:
            with self.get_connection() as conn:
                for chunk in pd.read_sql(
                    sql=text(query),
                    con=conn,
                    params=params,
                    chunksize=chunk_size
                ):
                    yield chunk
                    
        except Exception as e:
            logging.error(f"Streaming query failed: {e}")
            raise
            
    def execute_many(self,
                    query: str,
                    data: List[Dict[str, Any]],
                    batch_size: int = 1000,
                    counter: int = 0) -> int:
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query string
            data: List of parameter dictionaries
            batch_size: Number of records to insert per batch
            counter: Retry counter
            
        Returns:
            int: Number of affected rows
        """
        time.sleep(counter * 2)
        
        if counter > self.max_retries:
            logging.error(f"Max retries ({self.max_retries}) exceeded for batch insert")
            raise Exception(f"Batch insert failed after {self.max_retries} attempts")
            
        affected_rows = 0
        
        try:
            with self.get_connection() as conn:
                # Process in batches
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    result = conn.execute(text(query), batch)
                    affected_rows += result.rowcount
                    
                conn.commit()
                return affected_rows
                
        except Exception as e:
            logging.error(f"Batch execution failed: {e}")
            logging.info(f"Retrying... (attempt {counter + 1}/{self.max_retries})")
            return self.execute_many(query, data, batch_size, counter + 1)
            
    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """Check if a table exists in the database."""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = :table_name
            AND table_schema = COALESCE(:schema, current_schema())
        )
        """
        
        with self.get_connection() as conn:
            result = conn.execute(
                text(query),
                {'table_name': table_name, 'schema': schema}
            ).scalar()
            return result
            
    def get_table_columns(self, table_name: str, schema: Optional[str] = None) -> List[str]:
        """Get column names for a table."""
        query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :table_name
        AND table_schema = COALESCE(:schema, current_schema())
        ORDER BY ordinal_position
        """
        
        with self.get_connection() as conn:
            result = conn.execute(
                text(query),
                {'table_name': table_name, 'schema': schema}
            )
            return [row[0] for row in result]