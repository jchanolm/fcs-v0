"""
Neo4j connection and utility functions.
"""
import logging
from typing import List, Dict, Any
from neo4j import GraphDatabase
from app.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE

# Set up logging
logger = logging.getLogger(__name__)

# Global Neo4j driver variable
neo4j_driver = None

def init_neo4j():
    """Initialize Neo4j driver connection."""
    global neo4j_driver
    
    try:
        logger.info(f"Connecting to Neo4j with URI: {NEO4J_URI}")
        logger.info(f"Username: {NEO4J_USERNAME}")
        logger.info(f"Password: {'*' * len(NEO4J_PASSWORD) if NEO4J_PASSWORD else 'None'}")
        
        neo4j_driver = GraphDatabase.driver(
            NEO4J_URI, 
            auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
        )
        
        # Test the connection right away
        with neo4j_driver.session() as session:
            result = session.run("RETURN 1 as test")
            for record in result:
                logger.info(f"Neo4j connection test successful: {record['test']}")
        
        return True
    except Exception as e:
        logger.error(f"Neo4j connection error: {str(e)}")
        # Set neo4j_driver to None to indicate it's not available
        neo4j_driver = None
        logger.warning("Neo4j driver is not available - API will run in limited mode")
        return False

def execute_cypher(query, params=None):
    """Execute a Cypher query in Neo4j"""
    global neo4j_driver  # Explicitly use the global variable
    
    if neo4j_driver is None:
        logger.error("Neo4j driver is not initialized - cannot execute query")
        return []  # Return empty results instead of raising exception
        
    try:
        # Using None for database parameter will use the default database
        with neo4j_driver.session(database=NEO4J_DATABASE) as session:
            result = session.run(query, params)
            return list(result)
    except Exception as e:
        logger.error(f"Neo4j query execution error: {str(e)}")
        return []  # Return empty results on error

def close_neo4j_connection():
    """Close the Neo4j driver connection."""
    global neo4j_driver
    if neo4j_driver is not None:
        neo4j_driver.close()
        neo4j_driver = None
        logger.info("Neo4j connection closed")