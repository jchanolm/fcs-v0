import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator
from typing import Dict, Any, List, Optional
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
    token_addresses: List[str] = Field(..., max_items=25, description="List of token addresses to query (max 25)")
    
    @validator('token_addresses')
    def validate_token_addresses(cls, v):
        if not v:
            raise ValueError("At least one token address must be provided")
        if len(v) > 25:
            raise ValueError("Maximum of 25 token addresses allowed")
        return v

# Response models
class TokenData(BaseModel):
    address: str = Field(..., description="Token contract address")
    name: Optional[str] = Field(None, description="Token name")
    symbol: Optional[str] = Field(None, description="Token symbol")
    believerScore: float = Field(..., description="Calculated believer score for the token")
    holderCount: float = Field(..., description="Number of token holders")
    avgSocialCredScore: Optional[float] = Field(None, description="Average social credibility score")

class TokenResponseData(BaseModel):
    fcs_data: List[TokenData] = Field(..., description="List of token data with believer scores")

# Define routes
@app.get("/")
async def root():
    return {"message": "Token API is running"}

@app.post("/token-believer-score", response_model=TokenResponseData)
async def retrieve_token_believer_scores(request: TokensRequest) -> TokenResponseData:
    """Retrieve believer scores and supporting metadata for up to 25 Base token addresses"""
    try:
        # Query that accepts a list of token addresses
        query = """
      // Filter tokens by the provided addresses
      MATCH (token:Token)
      WHERE token.address IN $token_addresses
      
      // For each token, find all wallet holders
      MATCH (wallet:Wallet)-[:HOLDS]->(token)
      
      // Find all Warpcast accounts connected to these wallets (directly or through a path)
      WITH token, wallet
      OPTIONAL MATCH path = (wallet)-[:ACCOUNT*1..5]-(wc:Warpcast)
      
      // Group wallets by token and connected Warpcast account (if any)
      WITH token, wc, collect(DISTINCT wallet) AS wallet_group
      
      // Calculate weight for each group
      WITH token, wc, 
           CASE WHEN wc IS NULL THEN size(wallet_group) // Each unconnected wallet counts as 1
                ELSE 1 + coalesce(wc.fcCredScore, 0) // Connected wallets count as 1 + fcCredScore for the group
           END AS group_weight
      
      // Sum all weights for each token
      WITH token, sum(group_weight) AS weighted_holders, avg(wc.fcCredScore) as avgSocialCredScore
      
      // Return data for each token
      RETURN DISTINCT
       token.address as address, 
       token.name as name,
       token.symbol as symbol,
       tofloat(weighted_holders) as believerScore,
       tofloat(token.holderCount) as holderCount,
       avgSocialCredScore
    """

        requested_token_addresses = [x.lower() for x in request.token_addresses]
        params = {"token_addresses": requested_token_addresses}
        
        # Execute query
        print(f"Querying for tokens: {requested_token_addresses}")
        results = execute_cypher(query, params)
        
        # Process results
        if not results:
            raise HTTPException(status_code=404, detail="No tokens found with the provided addresses")
        
        # Convert Neo4j records to Pydantic models
        token_list = []
        for record in results:
            # Convert Neo4j record to dict and create TokenData object
            record_dict = dict(record)
            token_data = TokenData(**record_dict)
            token_list.append(token_data)
        
        response_data = TokenResponseData(fcs_data=token_list)
        
        return response_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Keep the original single token endpoint for backward compatibility
@app.post("/token", response_model=TokenResponseData)
async def get_token_data(request: TokensRequest) -> TokenResponseData:
    """Get data for a single token (redirects to /tokens endpoint)"""
    return await retrieve_token_believer_scores(request)

@app.on_event("shutdown")
async def shutdown_event():
    """Close Neo4j driver connection when app shuts down"""
    neo4j_driver.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)