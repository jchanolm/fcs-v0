"""
Reputation-related API endpoints.
"""
import logging
from fastapi import APIRouter, HTTPException, Path, Query
from app.models.reputation_models import ReputationRequest, ReputationResponse
from app.db.neo4j import execute_cypher, neo4j_driver
from app.config import FARSTORE_PASS
from typing import Dict, Any

# Set up logger for this module
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.get(
    "/user-reputation/{fid}",
    summary="Get user reputation metrics (GET method)",
    description="Retrieves detailed reputation metrics for a Farcaster user identified by FID via GET request.",
    response_model=ReputationResponse,
    responses={
        200: {"description": "Successfully retrieved reputation data", "model": ReputationResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "User not found with the provided FID"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_user_reputation_by_get(
    fid: int = Path(..., description="Farcaster ID (FID) to retrieve reputation for"),
    api_key: str = Query(..., description="API key for authentication")
) -> Dict[str, Any]:
    """
    GET endpoint to retrieve reputation data for a specific Farcaster user by FID.
    
    - Requires valid API key for authentication
    - Returns FCred score and rank 
    - Includes detailed metrics on engagement from other accounts
    """
    # Validate API key
    if api_key != FARSTORE_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    print(fid)
    print(api_key)
    
    try:
        # Execute query to get reputation data
        query = f"""
        match (wc:WarpcastAccount {fid: 190000})
        optional match (wc)<-[interact:REPLIED|RECASTED|LIKED|FOLLOWED]-(other:WarpcastAccount)
        where not wc.fid = other.fid 
        with wc.farconRank as rank, wc.farconScore as rawScore, 
             other,
             case when type(interact) = 'REPLIED' then 1 else 0 end as isReply,
             case when type(interact) = 'RECASTED' then 1 else 0 end as isRecast,
             case when type(interact) = 'LIKED' then 1 else 0 end as isLike,
             case when type(interact) = 'FOLLOWED' then 1 else 0 end as isFollow
        with rank, rawScore,
             sum(isReply) as replyCount,
             sum(isRecast) as recastCount,
             sum(isLike) as likeCount,
             sum(isFollow) as followCount,
             count(distinct(other)) as totalDistinctAccounts
        return {{
          fcCredRank: tointeger(rank), 
          fcCredScore: tofloat(rawScore), 
          engagedQualityAccounts: {{
            total: tointeger(totalDistinctAccounts),
            replied: tointeger(replyCount),
            recasted: tointeger(recastCount),
            liked: tointeger(likeCount),
            followed: tointeger(followCount)
          }}
        }} as data
        """
        
        # Execute the query - no parameters needed as FID is directly in the query
        results = execute_cypher(query)
        
        # Process results
        if not results or len(results) == 0 or not results[0].get("data"):
            raise HTTPException(status_code=404, detail=f"User not found with FID: {fid}")
        
        # Extract the data from the Neo4j result
        reputation_data = results[0].get("data")
        
        # Return the response
        return {"data": reputation_data}
    except Exception as e:
        logger.error(f"Error retrieving reputation data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.post(
    "/user-reputation",
    summary="Get user reputation metrics (POST method)",
    description="Retrieves detailed reputation metrics for a Farcaster user including their FCred score and engagement from quality accounts.",
    response_model=ReputationResponse,
    responses={
        200: {"description": "Successfully retrieved reputation data", "model": ReputationResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "User not found with the provided FID"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_user_reputation_by_post(request: ReputationRequest) -> Dict[str, Any]:
    """
    POST endpoint to retrieve reputation data for a specific Farcaster user.
    
    - Requires valid API key for authentication
    - Returns FCred score and rank
    - Includes detailed metrics on engagement from other accounts
    """
    # Validate API key
    if request.api_key != FARSTORE_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    try:
        # Execute query to get reputation data
        query = f"""
        match (wc:WarpcastAccount {{fid:{request.fid}}})
        optional match (wc)<-[interact:REPLIED|RECASTED|LIKED|FOLLOWED]-(other:WarpcastAccount)
        where not wc.fid = other.fid 
        with wc.farconRank as rank, wc.farconScore as rawScore, 
             other,
             case when type(interact) = 'REPLIED' then 1 else 0 end as isReply,
             case when type(interact) = 'RECASTED' then 1 else 0 end as isRecast,
             case when type(interact) = 'LIKED' then 1 else 0 end as isLike,
             case when type(interact) = 'FOLLOWED' then 1 else 0 end as isFollow
        with rank, rawScore,
             sum(isReply) as replyCount,
             sum(isRecast) as recastCount,
             sum(isLike) as likeCount,
             sum(isFollow) as followCount,
             count(distinct(other)) as totalDistinctAccounts
        return {{
          fcCredRank: tointeger(rank), 
          fcCredScore: tofloat(rawScore), 
          engagedQualityAccounts: {{
            total: tointeger(totalDistinctAccounts),
            replied: tointeger(replyCount),
            recasted: tointeger(recastCount),
            liked: tointeger(likeCount),
            followed: tointeger(followCount)
          }}
        }} as data
        """
        
        # Execute the query - no parameters needed as FID is directly in the query
        results = execute_cypher(query)
        
        # Process results
        if not results or len(results) == 0 or not results[0].get("data"):
            raise HTTPException(status_code=404, detail=f"User not found with FID: {request.fid}")
        
        # Extract the data from the Neo4j result
        reputation_data = results[0].get("data")
        
        # Return the response
        return {"data": reputation_data}
    except Exception as e:
        logger.error(f"Error retrieving reputation data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")