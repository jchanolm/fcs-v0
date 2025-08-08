# /app/api/endpoints/clankers.py
"""
Clankers-related API endpoints.
"""
import logging
from fastapi import APIRouter, HTTPException
from app.models.clankers_models import (
    ClankersHoldsRequest, ClankersHoldsResponse, TokenHoldingData, UserHolder
)
from app.db.neo4j import execute_cypher
from app.config import REPUTATION_PASS
from typing import Dict, Any

# Set up logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.post(
    "/holds-clankers",
    summary="Get tokens held by Farcaster users",
    description="Retrieves tokens held by a list of Farcaster users (FIDs) on a specific blockchain. Returns token details and holder information.",
    response_model=ClankersHoldsResponse,
    responses={
        200: {"description": "Successfully retrieved token holdings", "model": ClankersHoldsResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No token holdings found for the provided FIDs"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_holds_tokens(request: ClankersHoldsRequest) -> Dict[str, Any]:
    """
    Get tokens held by a list of Farcaster users.
    
    Retrieves all tokens held by the specified FIDs on the given blockchain,
    including token metadata and holder information with quotient scores.
    
    - Requires valid API key for authentication
    - Returns token details including holders and their quotient scores
    - Supports filtering by blockchain (default: arbitrum)
    """
    # Validate API key
    if request.api_key != REPUTATION_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
        logger.info(f"Processing holds-clankers request for {len(request.fids)} FIDs on chain: {request.chain}")
    
    try:
        # Build the Neo4j query
        query = """
        MATCH (wc:WarpcastAccount)-[:HOLDS]->(t:Token {chain: $chain})
        WHERE wc.fid IN $fids
        RETURN DISTINCT 
            t.address as address, 
            t.name as name, 
            t.description as description, 
            t.imageUrl as imageUrl,
            count(distinct(wc)) as count_holders,
            collect(distinct({
                fid: wc.fid, 
                username: wc.username, 
                pfpUrl: wc.pfpUrl, 
                quotientScore: wc.earlySummerNorm
            })) as holders
        ORDER BY count_holders DESC, t.name ASC
        """
        
        params = {
            "fids": request.fids,
            "chain": request.chain
        }
        
        logger.info(f"Executing Neo4j query for {len(request.fids)} FIDs on chain {request.chain}")
        
        # Execute the query
        results = execute_cypher(query, params)
        
        logger.info(f"Query results count: {len(results) if results else 0}")
        
        # Process results
        if not results:
            logger.warning(f"No token holdings found for the provided FIDs on chain {request.chain}")
            raise HTTPException(
                status_code=404, 
                detail=f"No token holdings found for the provided FIDs on chain {request.chain}"
            )
        
        # Convert results to TokenHoldingData objects
        tokens = []
        for record in results:
            # Convert holders to UserHolder objects
            holders = []
            for holder_data in record.get("holders", []):
                user_holder = UserHolder(
                    fid=holder_data.get("fid"),
                    username=holder_data.get("username") or "",
                    pfpUrl=holder_data.get("pfpUrl"),
                    quotientScore=holder_data.get("quotientScore")
                )
                holders.append(user_holder)
            
            # Create TokenHoldingData object
            token_data = TokenHoldingData(
                address=record.get("address"),
                name=record.get("name"),
                description=record.get("description"),
                imageUrl=record.get("imageUrl"),
                count_holders=record.get("count_holders", 0),
                holders=holders
            )
            tokens.append(token_data)
        
        logger.info(f"Returning {len(tokens)} tokens held by {len(request.fids)} users on {request.chain}")
        
        # Create response
        response = ClankersHoldsResponse(
            tokens=tokens,
            total_tokens=len(tokens),
            queried_fids=len(request.fids),
            chain=request.chain
        )
        
        return response.model_dump()
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error retrieving token holdings: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")