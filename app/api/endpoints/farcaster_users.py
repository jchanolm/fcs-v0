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
    logger.info(f"FID type: {type(request.fid)}, value: {request.fid}")
    
    # Validate API key
    if request.api_key != REPUTATION_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    try:
        # First, let's check if the user exists and has any follows
        check_query = """
        SELECT COUNT(*) as follow_count 
        FROM neynar.follows 
        WHERE fid = :fid OR target_fid = :fid
        """
        
        check_results = execute_postgres_query(check_query, {"fid": int(request.fid)})
        logger.info(f"Follow check results: {check_results}")
        
        # Query to find mutual followers with profile information
        # Using COALESCE to handle potential NULL values
        query = """
        SELECT DISTINCT
            t1.target_fid as fid,
            COALESCE(p.username, '') as username,
            COALESCE(p.pfp_url, '') as pfp_url
        FROM neynar.follows t1
        INNER JOIN neynar.follows t2 ON (t2.fid = t1.target_fid AND t2.target_fid = :fid)
        LEFT JOIN neynar.profiles p ON p.fid = t1.target_fid
        WHERE t1.fid = :fid
        """
        
        # Ensure FID is an integer
        params = {"fid": int(request.fid)}
        
        logger.info(f"Executing PostgreSQL query for mutual followers of FID: {request.fid}")
        logger.info(f"Query params: {params}")
        
        # Execute the query
        results = execute_postgres_query(query, params)
        
        logger.info(f"Query results: {results}")
        logger.info(f"Query results count: {len(results) if results else 0}")
        
        # Let's also try a simpler query to debug
        if not results:
            debug_query = """
            SELECT COUNT(*) as count FROM neynar.follows WHERE fid = :fid
            """
            debug_results = execute_postgres_query(debug_query, params)
            logger.info(f"Debug - User {request.fid} follows count: {debug_results}")
            
            debug_query2 = """
            SELECT COUNT(*) as count FROM neynar.follows WHERE target_fid = :fid
            """
            debug_results2 = execute_postgres_query(debug_query2, params)
            logger.info(f"Debug - User {request.fid} followers count: {debug_results2}")
        
        # Process results - don't throw 404 if no results
        mutual_followers = []
        if results:
            for record in results:
                # COALESCE handles everything in the query
                user_profile = UserProfile(
                    fid=record["fid"],
                    username=record["username"],
                    pfp_url=record["pfp_url"]
                )
                mutual_followers.append(user_profile)
        
        logger.info(f"Returning {len(mutual_followers)} mutual followers for FID {request.fid}")
        
        # Return the response - always return 200, even if no mutual followers found
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
        logger.error(f"Exception type: {type(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")