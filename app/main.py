"""
Main application module for the API.
This is the entry point that initializes the FastAPI app and includes all routes.
"""
import logging
import sys
import os
import builtins
from fastapi import FastAPI
from app.api.router import router
from app.db.mongo import init_mongodb
from app.db.neo4j import init_neo4j

# Enhanced logging setup - direct to stdout with DEBUG level
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True  # Override any previous configuration
)

# Store the original print function
original_print = builtins.print

# Create a new print function that always flushes
def flushing_print(*args, **kwargs):
    kwargs['flush'] = True
    return original_print(*args, **kwargs)

# Override the built-in print function
builtins.print = flushing_print

# Make sure all loggers are set to DEBUG level
for name in logging.root.manager.loggerDict:
    logging.getLogger(name).setLevel(logging.INFO)

# Initialize FastAPI
app = FastAPI(
    title="Quotient API", 
    description="API for querying token data, casts, and miniapps"
)

@app.on_event("startup")
async def startup_event():
    """Initialize database connections when app starts up"""
    print("=== API STARTING UP ===")
    
    # Initialize database connections
    mongo_success = init_mongodb()
    neo4j_success = init_neo4j()
    
    if not mongo_success:
        print("WARNING: MongoDB connection failed - API will run in limited mode")
    
    if not neo4j_success:
        print("WARNING: Neo4j connection failed - API will run in limited mode")

@app.on_event("shutdown")
async def shutdown_event():
    """Close database connections when app shuts down"""
    from app.db.neo4j import close_neo4j_connection
    
    print("=== SHUTTING DOWN API ===")
    close_neo4j_connection()

# Root endpoint
@app.get("/")
async def root():
    print("Root endpoint called")
    return {"message": "Quotient API is running"}

# Include all routes
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)