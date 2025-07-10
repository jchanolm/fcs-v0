# /app/api/endpoints/farcaster_users.py
"""
Farcaster users API endpoints.
"""
import logging
from fastapi import APIRouter, HTTPException, Path
from app.models.farcaster_models import MutualsResponse, UserProfile
from app.db.postgres import execute_postgres_query
from typing import Dict, Any, List

# Set up logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.get(
    "/farcaster-users/mutuals/{fid}",
    summary="Get mutual followers for a user",
    description="Retrieves users who mutually follow each other with the specified FID.",
    response_model=MutualsResponse,
    responses={
        200: {"description": "Successfully retrieved mutual followers", "model": MutualsResponse},
        404: {"description": "No mutual followers found for the provided FID"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_mutual_followers(
    fid: int = Path(..., description="Farcaster ID (FID) to find mutual followers for")
) -> Dict[str, Any]:
    """
    Get mutual followers for a specific Farcaster user by FID.
    
    Returns users who both follow the specified FID and are followed back by them.
    """
    logger.info(f"GET /farcaster-users/mutuals/{fid} - Processing mutual followers request")
    
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
        
        params = {"fid": fid}
        
        logger.info(f"Executing query for mutual followers of FID: {fid}")
        logger.info(f"Query parameters: {params}")
        
        # Execute the query
        results = execute_postgres_query(query, params)
        
        logger.info(f"Query results count: {len(results) if results else 0}")
        
        # Process results
        if not results:
            logger.warning(f"No mutual followers found for FID: {fid}")
            raise HTTPException(status_code=404, detail=f"No mutual followers found for FID: {fid}")
        
        # Convert results to UserProfile objects
        mutual_followers = []
        for record in results:
            user_profile = UserProfile(
                fid=record["fid"],
                username=record["username"] or "",
                pfp_url=record["pfp_url"] or ""
            )
            mutual_followers.append(user_profile)
        
        logger.info(f"Returning {len(mutual_followers)} mutual followers for FID {fid}")
        
        # Return the response
        return {
            "fid": fid,
            "mutual_followers": mutual_followers,
            "count": len(mutual_followers)
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error retrieving mutual followers: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")