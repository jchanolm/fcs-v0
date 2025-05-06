"""
Token-related API endpoints.
"""
import os
import logging
from fastapi import APIRouter, HTTPException
from app.models.token_models import (
    TokensRequest, TokenResponseData, TokenData,
    BelieversDataRequest, TopBelieversData
)
from app.db.neo4j import execute_cypher
from app.config import CLANK_PASS

# Set up logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.post(
    "/token-believer-score",
    summary="Get comprehensive token believer scores",
    description="Retrieves advanced believer scores with market cap adjustments and concentration metrics for tokens. Optionally filter by a specific token address.",
    responses={
        200: {"description": "Successfully retrieved token believer scores"},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No tokens found with the provided addresses"},
        500: {"description": "Internal Server Error"}
    }
)
async def retrieve_token_believer_scores(request: TokensRequest) -> dict:
    """
    Retrieve comprehensive believer scores and supporting metadata for token addresses
    
    - Requires valid API key for authentication
    - Returns normalized believer scores (0-70) with detailed metrics
    - Includes market cap adjustments, token concentration, and social metrics
    - Provides raw and adjusted scores for transparency
    - Optionally filter by a specific token address
    """
    # Validate API key
    if request.api_key != CLANK_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
        
    try:
        # Prepare parameters
        params = {}
        # Build the query based on whether a token address is provided
        if request.token_address:
            # If token_address is provided, add filter to the query
            query = """
            MATCH (token:Token)
            WHERE toLower(token.address) = toLower($token_address)
            RETURN DISTINCT
                token.address as address, 
                token.name as name,
                token.symbol as symbol,
                token.believerScore as believerScore,
                token.rawBelieverScore as rawBelieverScore,
                token.diversityAdjustedScore as diversityAdjustedScore,
                token.marketAdjustedScore as marketAdjustedScore,
                token.holderToMarketCapRatio as holderToMarketCapRatio,
                token.marketCap as marketCap,
                token.walletCount as walletCount,
                token.warpcastWallets as warpcastWallets,
                token.warpcastPercentage as warpcastPercentage,
                token.avgSocialCredScore as avgSocialCredScore,
                token.totalSupply as totalSupply
            """
            params["token_address"] = request.token_address.lower()
        else:
            # If no token_address, return all tokens
            query = """
            MATCH (token:Token)
            RETURN DISTINCT
                token.address as address, 
                token.name as name,
                token.symbol as symbol,
                token.believerScore as believerScore,
                token.rawBelieverScore as rawBelieverScore,
                token.diversityAdjustedScore as diversityAdjustedScore,
                token.marketAdjustedScore as marketAdjustedScore,
                token.holderToMarketCapRatio as holderToMarketCapRatio,
                token.marketCap as marketCap,
                token.walletCount as walletCount,
                token.warpcastWallets as warpcastWallets,
                token.warpcastPercentage as warpcastPercentage,
                token.avgSocialCredScore as avgSocialCredScore,
                token.totalSupply as totalSupply
            ORDER BY token.believerScore DESC
            """
        
        # Execute query
        logger.info(f"Querying for tokens with params: {params}")
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
        
        return response_data.model_dump()
    except Exception as e:
        logger.error(f"Error retrieving token believer scores: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.post(
    "/token-top-believers",
    summary="Top believers for token",
    description="Return top 25 believers for Product Clank-listed token",
    responses={
        200: {"description": "Successfully retrieved believers"},
        404: {"description": "No believers found for the token"},
        500: {"description": "Internal Server Error"}
    },
)
async def get_token_top_believers(request: BelieversDataRequest) -> dict:
    """
    Get top 25 believers for a specific token
    
    - Returns believers with their wallet and Warpcast account information
    """
    try:
        # Lowercase the token address
        token_address = request.token_address.lower()
        
        # Query to find top believers
        query = """
        MATCH (believerWallet:Wallet)-[r:HOLDS]->(token:Token {address:$token_address})
        MATCH (wc:Warpcast:Account)-[:ACCOUNT]->(believerWallet)  
        WHERE wc.fcCredScore is not null       
        ORDER BY wc.fcCredScore DESC LIMIT 100
        WITH wc, sum(tofloat(r.balance)) as balance
        RETURN {
            top_believers: COLLECT(DISTINCT({
                fid: tointeger(wc.fid),
                username: wc.username,
                bio: wc.bio,
                balance: balance,
                pfpUrl: wc.pfpUrl,
                fcred: wc.fcCredScore
            }))
        } as data"""
        
        params = {"token_address": token_address}
        # Execute query
        results = execute_cypher(query, params)
        
        # Process results
        if not results or len(results) == 0:
            raise HTTPException(status_code=404, detail="No believers found for the token")
        
        # Extract the data from the Neo4j result
        neo4j_data = results[0].get("data")
        believers_data = neo4j_data.get("top_believers", [])
        
        # Return the data in the expected format
        return {"believers": believers_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")