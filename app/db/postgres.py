# /app/db/postgres.py - Quick fix for the retry nonsense
"""
PostgreSQL connection and utility functions using SQLAlchemy.
"""
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from app.config import POSTGRES_CONNECTION_STRING

# Set up logging
logger = logging.getLogger(__name__)

# Global SQL utils instance
sql_utils = None

def init_postgres():
    """Initialize PostgreSQL connection - fail fast, no retries."""
    global sql_utils
    
    try:
        if not POSTGRES_CONNECTION_STRING:
            logger.warning("POSTGRES_CONNECTION_STRING not found")
            return False
            
        logger.info("Attempting PostgreSQL connection...")
        
        # Create engine with short timeout
        engine = create_engine(
            POSTGRES_CONNECTION_STRING,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=5,  # Short timeout
            pool_recycle=3600,
            connect_args={"connect_timeout": 5}  # 5 second connection timeout
        )
        
        # Quick test with timeout
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                sql_utils = SimpleSQL(engine)
                logger.info("PostgreSQL connection successful")
                return True
        
        return False
        
    except Exception as e:
        logger.warning(f"PostgreSQL connection failed: {str(e)} - continuing without PostgreSQL")
        sql_utils = None
        return False

class SimpleSQL:
    """Simple SQL class without retry bullshit."""
    
    def __init__(self, engine):
        self.engine = engine
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query - no retries, fail fast."""
        try:
            with self.get_connection() as conn:
                result = conn.execute(text(query), params or {})
                rows = []
                for row in result:
                    rows.append(dict(row._mapping))
                return rows
        except Exception as e:
            logger.error(f"PostgreSQL query failed: {e}")
            return []

def execute_postgres_query(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Execute a PostgreSQL query."""
    if sql_utils is None:
        logger.error("PostgreSQL not initialized")
        return []
    
    return sql_utils.execute_query(query, params)

def close_postgres_connection():
    """Close the PostgreSQL connection."""
    global sql_utils
    if sql_utils is not None:
        sql_utils = None
        logger.info("PostgreSQL connection closed")