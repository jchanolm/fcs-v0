# /app/api/endpoints/leaderboard.py
"""
Leaderboard API endpoints - OPTIMIZED VERSION with FCS enrichment
"""
import logging
from fastapi import APIRouter, HTTPException, Query, Path
from app.models.leaderboard_models import LeaderboardResponse, UserLeaderboardResponse
from app.db.postgres import execute_postgres_query
from app.config import TEST_LEADERBOARD_KEY
from typing import Dict, Any, List, Optional

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

def get_fid_from_wallet(wallet_address: str) -> Optional[int]:
    """
    Look up FID from wallet address using neynar.verifications.
    
    Args:
        wallet_address: Ethereum wallet address
        
    Returns:
        FID if found, None otherwise
    """
    query = """
    SELECT fid 
    FROM neynar.verifications 
    WHERE LOWER(address) = LOWER(:wallet_address)
    LIMIT 1
    """
    
    try:
        result = execute_postgres_query(query, {"wallet_address": wallet_address})
        if result and len(result) > 0:
            return result[0].get('fid')
        return None
    except Exception as e:
        logger.error(f"Error looking up FID for wallet {wallet_address}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error querying verifications: {str(e)}"
        )

@router.get(
    "/leaderboard/{leaderboard_name}",
    summary="Get full leaderboard",
    description="Retrieve the complete leaderboard for the specified leaderboard name. Returns the latest snapshot by default, or all historical snapshots if run_timestamp=all.",
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
    api_key: str = Query(..., description="API key for authentication"),
    run_timestamp: str = Query(None, description="Optional: 'all' to get all historical snapshots, omit for latest only")
) -> Dict[str, Any]:
    """
    GET endpoint to retrieve a full leaderboard.

    - Requires valid API key for authentication
    - Returns all entries from the latest leaderboard snapshot by default
    - Use run_timestamp=all to retrieve all historical snapshots
    - Leaderboard name corresponds to table name in the leaderboards schema
    - Each entry includes 'addresses' field with all verified wallet addresses
    - Each entry includes 'quotient_score' and 'quotient_rank' from farcaster.fcs_scores
    """
    # Validate API key
    if not validate_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    logger.info(f"GET /leaderboard/{leaderboard_name} - Fetching full leaderboard (run_timestamp={run_timestamp})")

    try:
        # Determine if we're fetching all timestamps or just the latest
        if run_timestamp and run_timestamp.lower() == "all":
            # Query all entries across all timestamps with FCS scores and addresses
            query = f"""
            SELECT 
                l.*,
                s.quotient_score,
                s.quotient_rank,
                a.addresses
            FROM leaderboards.{leaderboard_name} l
            LEFT JOIN LATERAL (
                SELECT 
                    fc_cred_score_norm as quotient_score,
                    fc_cred_rank as quotient_rank
                FROM farcaster.fcs_scores
                WHERE fid = l.fid
                ORDER BY run_timestamp DESC
                LIMIT 1
            ) s ON true
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    ARRAY_AGG(DISTINCT '0x' || encode(address, 'hex')),
                    ARRAY[]::text[]
                ) as addresses
                FROM neynar.verifications
                WHERE fid = l.fid
            ) a ON true
            ORDER BY l.run_timestamp DESC, l.rank ASC
            """
            params = {}
            results = execute_postgres_query(query, params)

            if not results:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for leaderboard '{leaderboard_name}'"
                )

            # Get unique timestamps for metadata
            timestamps = sorted(list(set([r.get('run_timestamp') for r in results])), reverse=True)

            logger.info(f"Retrieved {len(results)} entries across {len(timestamps)} timestamps from leaderboard '{leaderboard_name}'")

            return {
                "leaderboard_name": leaderboard_name,
                "data": results,
                "count": len(results),
                "run_timestamp": None,
                "run_timestamps": timestamps
            }
        else:
            # Get the latest run_timestamp
            max_timestamp = get_latest_run_timestamp(leaderboard_name)

            if max_timestamp is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Leaderboard '{leaderboard_name}' not found or is empty"
                )

            # Query all entries for the latest timestamp with FCS scores and addresses
            query = f"""
            SELECT 
                l.*,
                s.quotient_score,
                s.quotient_rank,
                a.addresses
            FROM leaderboards.{leaderboard_name} l
            LEFT JOIN LATERAL (
                SELECT 
                    fc_cred_score_norm as quotient_score,
                    fc_cred_rank as quotient_rank
                FROM farcaster.fcs_scores
                WHERE fid = l.fid
                ORDER BY run_timestamp DESC
                LIMIT 1
            ) s ON true
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    ARRAY_AGG(DISTINCT '0x' || encode(address, 'hex')),
                    ARRAY[]::text[]
                ) as addresses
                FROM neynar.verifications
                WHERE fid = l.fid
            ) a ON true
            WHERE l.run_timestamp = :max_timestamp
            ORDER BY l.rank ASC
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
    description="Retrieve a specific user's entry from a leaderboard. Can lookup by FID or wallet address. Returns latest entry by default, or all historical entries if run_timestamp=all.",
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
    fid: Optional[int] = Query(None, description="Farcaster ID (FID) of the user to look up"),
    wallet_address: Optional[str] = Query(None, description="Wallet address to look up (will resolve to FID)"),
    run_timestamp: str = Query(None, description="Optional: 'all' to get all historical entries, omit for latest only")
) -> Dict[str, Any]:
    """
    GET endpoint to retrieve a specific user's leaderboard entry.

    - Requires valid API key for authentication
    - Supports lookup by FID or wallet address (must provide one)
    - Returns the user's entry from the latest leaderboard snapshot by default
    - Use run_timestamp=all to retrieve all historical entries for this user
    - Leaderboard name corresponds to table name in the leaderboards schema
    - Each entry includes 'addresses' field with all verified wallet addresses
    - Each entry includes 'quotient_score' and 'quotient_rank' from farcaster.fcs_scores
    """
    # Validate API key
    if not validate_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    # Validate that at least one identifier is provided
    if not fid and not wallet_address:
        raise HTTPException(
            status_code=400,
            detail="Must provide either 'fid' or 'wallet_address' parameter"
        )

    # If wallet_address provided, resolve to FID
    user_identifier = ""
    if wallet_address:
        logger.info(f"Looking up FID for wallet address: {wallet_address}")
        fid = get_fid_from_wallet(wallet_address)
        if fid is None:
            logger.info(f"Wallet address {wallet_address} not found in verifications")
            return {
                "leaderboard_name": leaderboard_name,
                "user_identifier": f"wallet:{wallet_address}",
                "data": None,
                "found": False,
                "run_timestamp": None,
                "run_timestamps": []
            }
        user_identifier = f"wallet:{wallet_address} (fid:{fid})"
        logger.info(f"Resolved wallet {wallet_address} to FID {fid}")
    else:
        user_identifier = f"fid:{fid}"

    logger.info(f"GET /leaderboard/{leaderboard_name}/user?{user_identifier} - Fetching user entry (run_timestamp={run_timestamp})")

    try:
        # Determine if we're fetching all timestamps or just the latest
        if run_timestamp and run_timestamp.lower() == "all":
            # Query all entries for this user across all timestamps with FCS scores and addresses
            query = f"""
            SELECT 
                l.*,
                s.quotient_score,
                s.quotient_rank,
                a.addresses
            FROM leaderboards.{leaderboard_name} l
            LEFT JOIN LATERAL (
                SELECT 
                    fc_cred_score_norm as quotient_score,
                    fc_cred_rank as quotient_rank
                FROM farcaster.fcs_scores
                WHERE fid = l.fid
                ORDER BY run_timestamp DESC
                LIMIT 1
            ) s ON true
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    ARRAY_AGG(DISTINCT '0x' || encode(address, 'hex')),
                    ARRAY[]::text[]
                ) as addresses
                FROM neynar.verifications
                WHERE fid = l.fid
            ) a ON true
            WHERE l.fid = :fid
            ORDER BY l.run_timestamp DESC
            """
            params = {"fid": fid}
            results = execute_postgres_query(query, params)

            if not results or len(results) == 0:
                logger.info(f"User with {user_identifier} not found in any snapshot of leaderboard '{leaderboard_name}'")
                return {
                    "leaderboard_name": leaderboard_name,
                    "user_identifier": user_identifier,
                    "data": None,
                    "found": False,
                    "run_timestamp": None,
                    "run_timestamps": []
                }

            # Get unique timestamps for this user
            timestamps = sorted(list(set([r.get('run_timestamp') for r in results])), reverse=True)

            logger.info(f"Retrieved {len(results)} entries across {len(timestamps)} timestamps for {user_identifier} from leaderboard '{leaderboard_name}'")

            return {
                "leaderboard_name": leaderboard_name,
                "user_identifier": user_identifier,
                "data": results,
                "found": True,
                "run_timestamp": None,
                "run_timestamps": timestamps
            }
        else:
            # Get the latest run_timestamp
            max_timestamp = get_latest_run_timestamp(leaderboard_name)

            if max_timestamp is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Leaderboard '{leaderboard_name}' not found or is empty"
                )

            # Query the specific user's entry for the latest timestamp with FCS scores and addresses
            query = f"""
            SELECT 
                l.*,
                s.quotient_score,
                s.quotient_rank,
                a.addresses
            FROM leaderboards.{leaderboard_name} l
            LEFT JOIN LATERAL (
                SELECT 
                    fc_cred_score_norm as quotient_score,
                    fc_cred_rank as quotient_rank
                FROM farcaster.fcs_scores
                WHERE fid = l.fid
                ORDER BY run_timestamp DESC
                LIMIT 1
            ) s ON true
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    ARRAY_AGG(DISTINCT '0x' || encode(address, 'hex')),
                    ARRAY[]::text[]
                ) as addresses
                FROM neynar.verifications
                WHERE fid = l.fid
            ) a ON true
            WHERE l.run_timestamp = :max_timestamp
            AND l.fid = :fid
            """

            params = {"max_timestamp": max_timestamp, "fid": fid}
            results = execute_postgres_query(query, params)

            if not results or len(results) == 0:
                logger.info(f"User with {user_identifier} not found in leaderboard '{leaderboard_name}'")
                return {
                    "leaderboard_name": leaderboard_name,
                    "user_identifier": user_identifier,
                    "data": None,
                    "found": False,
                    "run_timestamp": max_timestamp
                }

            logger.info(f"Retrieved entry for {user_identifier} from leaderboard '{leaderboard_name}'")

            return {
                "leaderboard_name": leaderboard_name,
                "user_identifier": user_identifier,
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