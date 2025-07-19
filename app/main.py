# /app/main.py
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
from app.db.neo4j import init_neo4j
from app.db.postgres import init_postgres

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
    description="API for querying token data, casts, miniapps, and Farcaster users"
)

@app.on_event("startup")
async def startup_event():
    """Initialize database connections when app starts up"""
    print("=== API STARTING UP ===")
    
    # Neo4j (required for most endpoints)
    neo4j_success = init_neo4j()
    print(f"Neo4j: {'✓' if neo4j_success else '✗'}")
    
    # PostgreSQL (only for some endpoints, don't let it block startup)
    postgres_success = init_postgres()
    print(f"PostgreSQL: {'✓' if postgres_success else '✗'}")
    
    
    print("=== API READY ===")

@app.on_event("shutdown")
async def shutdown_event():
    """Close database connections when app shuts down"""
    from app.db.neo4j import close_neo4j_connection
    from app.db.postgres import close_postgres_connection
    
    print("=== SHUTTING DOWN API ===")
    try:
        close_neo4j_connection()
    except:
        pass
    try:
        close_postgres_connection()
    except:
        pass

# Root endpoint
@app.get("/")
async def root():
    print("Root endpoint called")
    return {"message": "Quotient API is running"}

# Include all routes with v1 prefix
app.include_router(router, prefix="/v1")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)