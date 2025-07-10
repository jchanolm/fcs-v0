# /app/api/endpoints/farcaster_users.py
"""
Farcaster users API endpoints.
"""
import logging
from fastapi import APIRouter, HTTPException, Path, Query
from app.models.farcaster_models import MutualsResponse, MutualsRequest, UserProfile
from app.db.postgres import execute_postgres_query
from app.config import REPUTATION_PASS
from typing import Dict, Any, List

# Set up logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.post(
    "/farcaster-users/mutuals",
    summary="Get mutual followers for a user",
    description="Retrieves users who mutually follow each other with the specified FID. API key required for authentication.",
    response_model=MutualsResponse,
    responses={
        200: {"description": "Successfully retrieved mutual followers", "model": MutualsResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No mutual followers found for the provided FID"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_mutual_followers(request: MutualsRequest) -> Dict[str, Any]:
    """
    Get mutual followers for a specific Farcaster user by FID.
    
    Returns users who both follow the specified FID and are followed back by them.
    Requires valid API key for authentication.
    """
    logger.info(f"POST /farcaster-users/mutuals - Processing mutual followers request for FID: {request.fid}")
    
    # Validate API key
    if request.api_key != REPUTATION_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    try:
        # Query to find mutual followers with profile information
        query = """
        SELECT DISTINCT
            t1.target_fid as fid,
            p.username,
            p.pfp_url
        FROM neynar.follows t1
        INNER JOIN neynar.follows t2 ON (t2.fid = t1.target_fid AND t2.target_fid = :fid)
        INNER JOIN neynar.profiles p ON p.fid = t1.target_fid
        WHERE t1.fid = :fid
        ORDER BY p.username
        """
        
        params = {"fid": request.fid}
        
        logger.info(f"Executing query for mutual followers of FID: {request.fid}")
        
        # Execute the query
        results = execute_postgres_query(query, params)
        
        logger.info(f"Query results count: {len(results) if results else 0}")
        
        # Process results
        if not results:
            logger.warning(f"No mutual followers found for FID: {request.fid}")
            raise HTTPException(status_code=404, detail=f"No mutual followers found for FID: {request.fid}")
        
        # Convert results to UserProfile objects
        mutual_followers = []
        for record in results:
            user_profile = UserProfile(
                fid=record["fid"],
                username=record["username"] or "",
                pfp_url=record["pfp_url"] or ""
            )
            mutual_followers.append(user_profile)
        
        logger.info(f"Returning {len(mutual_followers)} mutual followers for FID {request.fid}")
        
        # Return the response
        return {
            "fid": request.fid,
            "mutual_followers": mutual_followers,
            "count": len(mutual_followers)
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error retrieving mutual followers: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")