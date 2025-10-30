# /app/api/endpoints/leaderboard.py
"""
Leaderboard API endpoints.
"""
import logging
from fastapi import APIRouter, HTTPException, Query, Path
from app.models.leaderboard_models import LeaderboardResponse, UserLeaderboardResponse
from app.db.postgres import execute_postgres_query
from app.config import TEST_LEADERBOARD_KEY
from typing import Dict, Any, List

# Set up logger for this module
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

def validate_api_key(api_key: str) -> bool:
    """Validate the provided API key."""
    if not TEST_LEADERBOARD_KEY:
        logger.error("TEST_LEADERBOARD_KEY not configured")
        return False
    return api_key == TEST_LEADERBOARD_KEY

def get_latest_run_timestamp(leaderboard_name: str) -> Any:
    """
    Get the latest run_timestamp for a leaderboard.

    Args:
        leaderboard_name: Name of the leaderboard table

    Returns:
        The latest run_timestamp or None if not found
    """
    query = f"SELECT MAX(run_timestamp) as max_timestamp FROM leaderboards.{leaderboard_name}"

    try:
        result = execute_postgres_query(query)
        if result and len(result) > 0:
            return result[0].get('max_timestamp')
        return None
    except Exception as e:
        logger.error(f"Error getting latest timestamp for {leaderboard_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error querying leaderboard: {str(e)}"
        )

@router.get(
    "/leaderboard/{leaderboard_name}",
    summary="Get full leaderboard",
    description="Retrieve the complete leaderboard for the specified leaderboard name. Returns the latest snapshot based on run_timestamp.",
    response_model=LeaderboardResponse,
    responses={
        200: {"description": "Successfully retrieved leaderboard data", "model": LeaderboardResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "Leaderboard not found or empty"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_leaderboard(
    leaderboard_name: str = Path(..., description="Name of the leaderboard to retrieve"),
    api_key: str = Query(..., description="API key for authentication")
) -> Dict[str, Any]:
    """
    GET endpoint to retrieve a full leaderboard.

    - Requires valid API key for authentication
    - Returns all entries from the latest leaderboard snapshot
    - Leaderboard name corresponds to table name in the leaderboards schema
    """
    # Validate API key
    if not validate_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    logger.info(f"GET /leaderboard/{leaderboard_name} - Fetching full leaderboard")

    try:
        # Get the latest run_timestamp
        max_timestamp = get_latest_run_timestamp(leaderboard_name)

        if max_timestamp is None:
            raise HTTPException(
                status_code=404,
                detail=f"Leaderboard '{leaderboard_name}' not found or is empty"
            )

        # Query all entries for the latest timestamp
        query = f"""
        SELECT * FROM leaderboards.{leaderboard_name}
        WHERE run_timestamp = :max_timestamp
        """

        params = {"max_timestamp": max_timestamp}
        results = execute_postgres_query(query, params)

        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for leaderboard '{leaderboard_name}'"
            )

        logger.info(f"Retrieved {len(results)} entries from leaderboard '{leaderboard_name}'")

        return {
            "leaderboard_name": leaderboard_name,
            "data": results,
            "count": len(results),
            "run_timestamp": max_timestamp
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving leaderboard '{leaderboard_name}': {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )

@router.get(
    "/leaderboard/{leaderboard_name}/user",
    summary="Get user's leaderboard entry",
    description="Retrieve a specific user's entry from a leaderboard. Currently supports lookup by FID.",
    response_model=UserLeaderboardResponse,
    responses={
        200: {"description": "Successfully retrieved user leaderboard data", "model": UserLeaderboardResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "Leaderboard not found or user not in leaderboard"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_user_leaderboard(
    leaderboard_name: str = Path(..., description="Name of the leaderboard to retrieve"),
    api_key: str = Query(..., description="API key for authentication"),
    fid: int = Query(..., description="Farcaster ID (FID) of the user to look up")
) -> Dict[str, Any]:
    """
    GET endpoint to retrieve a specific user's leaderboard entry.

    - Requires valid API key for authentication
    - Currently supports lookup by FID (Farcaster ID)
    - Returns the user's entry from the latest leaderboard snapshot
    - Leaderboard name corresponds to table name in the leaderboards schema
    """
    # Validate API key
    if not validate_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    logger.info(f"GET /leaderboard/{leaderboard_name}/user?fid={fid} - Fetching user entry")

    try:
        # Get the latest run_timestamp
        max_timestamp = get_latest_run_timestamp(leaderboard_name)

        if max_timestamp is None:
            raise HTTPException(
                status_code=404,
                detail=f"Leaderboard '{leaderboard_name}' not found or is empty"
            )

        # Query the specific user's entry for the latest timestamp
        query = f"""
        SELECT * FROM leaderboards.{leaderboard_name}
        WHERE run_timestamp = :max_timestamp
        AND fid = :fid
        """

        params = {"max_timestamp": max_timestamp, "fid": fid}
        results = execute_postgres_query(query, params)

        if not results or len(results) == 0:
            logger.info(f"User with FID {fid} not found in leaderboard '{leaderboard_name}'")
            return {
                "leaderboard_name": leaderboard_name,
                "user_identifier": f"fid:{fid}",
                "data": None,
                "found": False,
                "run_timestamp": max_timestamp
            }

        logger.info(f"Retrieved entry for FID {fid} from leaderboard '{leaderboard_name}'")

        return {
            "leaderboard_name": leaderboard_name,
            "user_identifier": f"fid:{fid}",
            "data": results[0],
            "found": True,
            "run_timestamp": max_timestamp
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving user entry from leaderboard '{leaderboard_name}': {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
