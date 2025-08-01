# /app/api/endpoints/reputation.py
"""
Reputation-related API endpoints.
"""
import logging
from fastapi import APIRouter, HTTPException, Path, Query
from app.models.reputation_models import ReputationRequest, ReputationResponse
from app.db.neo4j import execute_cypher
from app.config import REPUTATION_PASS
from typing import Dict, Any

# Set up logger for this module
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.post(
    "/user-reputation",
    summary="Get user reputation metrics for multiple users",
    description="Retrieves quotient scores and ranking for up to 1000 Farcaster users.",
    response_model=ReputationResponse,
    responses={
        200: {"description": "Successfully retrieved reputation data", "model": ReputationResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No users found with the provided FIDs"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_user_reputation_by_post(request: ReputationRequest) -> Dict[str, Any]:
    """
    POST endpoint to retrieve reputation data for multiple Farcaster users.
    
    - Requires valid API key for authentication
    - Returns quotient score, raw score, and ranking for each user
    - Accepts up to 1000 FIDs per request
    """
    # Validate API key
    if request.api_key != REPUTATION_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    logger.info(f"POST /user-reputation - Processing reputation request for {len(request.fids)} FIDs")
    
    try:
        # Execute query to get reputation data for multiple users
        # Use parameterized query for safety and proper type handling
        query = """
        MATCH (wc:WarpcastAccount)
        WHERE wc.fid IN $fids
        RETURN {
          fid: wc.fid,
          username: COALESCE(wc.username, ''),
          quotientScore: COALESCE(wc.earlySummerNorm, null),
          quotientScoreRaw: COALESCE(wc.earlySummer, null),
          quotientRank: COALESCE(wc.earlySummerRank, null),
          quotientProfileUrl: CASE 
            WHEN wc.username IS NOT NULL 
            THEN "farcaster.quotient.social/user/" + wc.username
            ELSE ''
          END
        } as data
        ORDER BY wc.earlySummerRank ASC
        """
        
        # Parameters for the query
        params = {"fids": request.fids}
        
        logger.info(f"Executing Neo4j query for FIDs: {request.fids}")
        
        # Execute the query with parameters
        results = execute_cypher(query, params)
        
        logger.info(f"Query results count: {len(results) if results else 0}")
        
        # Process results - always return what we found, even if empty
        reputation_list = []
        if results:
            for result in results:
                if result.get("data"):
                    data = result.get("data")
                    # Additional validation to ensure no None values for required string fields
                    if data.get("username") is None:
                        data["username"] = ""
                    if data.get("quotientProfileUrl") is None:
                        data["quotientProfileUrl"] = ""
                    # Keep numeric fields as None if they're null - the model allows Optional
                    reputation_list.append(data)
        
        logger.info(f"Returning reputation data for {len(reputation_list)} users")
        
        # Return the response - always return 200, even if no results found
        return {
            "data": reputation_list,
            "count": len(reputation_list)
        }
    except Exception as e:
        logger.error(f"Error retrieving reputation data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")