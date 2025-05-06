"""
MongoDB connection and utility functions.
"""
import logging
from typing import List, Dict, Any
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from app.config import MONGO_DB_URL, MONGO_DB_NAME

# Set up logging
logger = logging.getLogger(__name__)

# Initialize MongoDB clients as None
mongo_client = None
async_mongo_client = None
db = None
async_db = None

def init_mongodb():
    """Initialize MongoDB connections."""
    global mongo_client, async_mongo_client, db, async_db
    
    try:
        # Synchronous client for non-API operations
        mongo_client = MongoClient(MONGO_DB_URL)
        
        # Async client for API operations
        async_mongo_client = AsyncIOMotorClient(MONGO_DB_URL)
        
        # Database references
        db = mongo_client[MONGO_DB_NAME]
        async_db = async_mongo_client[MONGO_DB_NAME]
        
        # Test the connection
        db_info = mongo_client.server_info()
        logger.info(f"Connected to MongoDB: {db_info.get('version')}")
        
        # List available collections
        collection_names = db.list_collection_names()
        logger.info(f"MongoDB collections: {', '.join(collection_names)}")
        
        # Check if we have the casts collection
        if 'casts' not in collection_names:
            logger.warning("Casts collection not found in MongoDB")
        
        return True
    except Exception as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        # Set these to None to indicate they're not available
        mongo_client = None
        async_mongo_client = None
        db = None
        async_db = None
        return False

async def search_mongo_casts(query: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Query the Atlas Search index "text" on the `casts` collection."""
    if async_db is None:
        logger.warning("MongoDB connection not initialised – skipping search")
        return []

    search_pipeline = [
        {
            "$search": {
                "index": "text",           # name of your Atlas index
                "text": {
                    "query": query,
                    "path": {"wildcard": "*"}  # search across every indexed field
                },
            }
        },
        {"$limit": limit},
        {"$addFields": {"score": {"$meta": "searchScore"}}},
    ]

    results = await async_db.casts.aggregate(search_pipeline).to_list(length=limit)
    logger.info("Atlas Search returned %s casts", len(results))
    return results

async def get_casts_by_hashes(hashes: List[str]) -> List[Dict[str, Any]]:
    """
    Retrieve casts by their hash values
    
    Args:
        hashes: List of cast hash values to retrieve
        
    Returns:
        List of matching cast documents
    """
    try:
        logger.info(f"Retrieving {len(hashes)} casts from MongoDB by hash")
        
        if not hashes:
            return []
            
        # Query casts by hash
        results = await async_db.casts.find({"hash": {"$in": hashes}}).to_list(length=len(hashes))
        
        logger.info(f"Retrieved {len(results)} casts from MongoDB by hash")
        
        # Convert MongoDB _id to string
        for doc in results:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
                
        return results
        
    except Exception as e:
        logger.error(f"Error retrieving casts by hash: {str(e)}")
        logger.exception("MongoDB get_casts_by_hashes exception:")
        return []