import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator
from typing import Dict, Any, List
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Neo4j Configuration
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
# Database will be None, which will use the default database
NEO4J_DATABASE = None

# Initialize Neo4j driver
neo4j_driver = GraphDatabase.driver(
    NEO4J_URI, 
    auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
)

# Initialize FastAPI
app = FastAPI(title="Token API", description="API for querying token data from Neo4j")

def execute_cypher(query, params=None):
    """Execute a Cypher query in Neo4j"""
    # Using None for database parameter will use the default database
    with neo4j_driver.session(database=NEO4J_DATABASE) as session:
        result = session.run(query, params)
        return list(result)

# Request models
class TokensRequest(BaseModel):
    token_addresses: List[str] = Field(..., max_items=25)
    
    @validator('token_addresses')
    def validate_token_addresses(cls, v):
        if not v:
            raise ValueError("At least one token address must be provided")
        if len(v) > 25:
            raise ValueError("Maximum of 25 token addresses allowed")
        return v

# Define routes
@app.get("/")
async def root():
    return {"message": "Token API is running"}


@app.post("/tokens-fc")
async def get_tokens_data(request: TokensRequest) -> Dict[str, Any]:
    """Get token data from Neo4j based on token addresses (up to 25)"""
    try:
        # Query that accepts a list of token addresses
        query = """
        MATCH (wallet:Wallet)-[r:HELD]->(token:Token)
        WHERE token.address in $token_addresses
        MATCH (wallet)-[rr:ACCOUNT]-(wc:Warpcast)
        OPTIONAL MATCH (wc)-[:ACCOUNT]-(otherWallet:Wallet)
        OPTIONAL MATCH (wc)-[:ACCOUNT]-(account:Account)
        WHERE NOT account:Wallet
        WITH token, wc, sum(tofloat(r.balance)) as token_balance, sum(tofloat(wallet.balance)) as tokens_held, wc.fcCredScore as fcs 
        WITH token, avg(fcs) as avg_fcs
        RETURN COLLECT(DISTINCT({
            token: token.address, 
            avg_fcs: avg_fcs
        }))  as token_fcs_data
              """

        requested_token_addresses = [x.lower() for x in request.token_addresses]
        params = {"token_addresses": requested_token_addresses}
        
        # Execute query
        results = execute_cypher(query, params)
        
        # Process results
        if not results:
            raise HTTPException(status_code=404, detail="No tokens found")
        
        response_data = {
            "fcs_data": results,
        }
        
        return response_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Keep the original single token endpoint for backward compatibility
@app.post("/token")
async def get_token_data(request: TokensRequest) -> Dict[str, Any]:
    """Get data for a single token (redirects to /tokens endpoint)"""
    return await get_tokens_data(request)

@app.on_event("shutdown")
async def shutdown_event():
    """Close Neo4j driver connection when app shuts down"""
    neo4j_driver.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)