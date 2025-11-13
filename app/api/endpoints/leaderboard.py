# /app/api/endpoints/leaderboard.py
"""
Leaderboard API endpoints - OPTIMIZED VERSION
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

def get_verified_addresses_batch(fids: List[int]) -> Dict[int, List[str]]:
    """
    Get all verified wallet addresses for multiple FIDs in a single query.
    
    Args:
        fids: List of Farcaster IDs
        
    Returns:
        Dictionary mapping FID to list of verified wallet addresses (as hex strings)
    """
    if not fids:
        return {}
    
    # Remove duplicates
    unique_fids = list(set(fids))
    
    # Use encode(address, 'hex') to convert bytea to hex string
    query = """
    SELECT fid, '0x' || encode(address, 'hex') as address
    FROM neynar.verifications 
    WHERE fid = ANY(:fids)
    """
    
    try:
        result = execute_postgres_query(query, {"fids": unique_fids})
        
        # Group addresses by FID
        fid_to_addresses = {}
        for row in result:
            fid = row.get('fid')
            address = row.get('address')
            if fid not in fid_to_addresses:
                fid_to_addresses[fid] = []
            fid_to_addresses[fid].append(address)
        
        logger.info(f"Fetched addresses for {len(unique_fids)} FIDs in single query")
        return fid_to_addresses
    except Exception as e:
        logger.error(f"Error getting verified addresses for FIDs: {e}")
        return {}

def get_verified_addresses(fid: int) -> List[str]:
    """
    Get all verified wallet addresses for a single FID.
    Used for single user lookups.
    
    Args:
        fid: Farcaster ID
        
    Returns:
        List of verified wallet addresses (as hex strings)
    """
    # Use encode(address, 'hex') to convert bytea to hex string
    query = """
    SELECT '0x' || encode(address, 'hex') as address
    FROM neynar.verifications 
    WHERE fid = :fid
    """
    
    try:
        result = execute_postgres_query(query, {"fid": fid})
        if result:
            return [r.get('address') for r in result]
        return []
    except Exception as e:
        logger.error(f"Error getting verified addresses for FID {fid}: {e}")
        return []

def enrich_with_addresses(data: Any) -> Any:
    """
    Enrich leaderboard data with verified wallet addresses.
    OPTIMIZED: Uses batch query for lists to avoid N+1 problem.
    
    Args:
        data: Single entry (Dict) or list of entries (List[Dict])
        
    Returns:
        Enriched data with 'addresses' field added
    """
    if isinstance(data, dict):
        # Single entry - use simple query
        fid = data.get('fid')
        if fid:
            data['addresses'] = get_verified_addresses(fid)
        return data
    elif isinstance(data, list):
        # Multiple entries - use batch query to avoid N+1 problem
        fids = [entry.get('fid') for entry in data if entry.get('fid')]
        
        if fids:
            # Get all addresses in ONE query
            fid_to_addresses = get_verified_addresses_batch(fids)
            
            # Assign addresses to each entry
            for entry in data:
                fid = entry.get('fid')
                if fid:
                    entry['addresses'] = fid_to_addresses.get(fid, [])
        
        return data
    return data

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
    """
    # Validate API key
    if not validate_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    logger.info(f"GET /leaderboard/{leaderboard_name} - Fetching full leaderboard (run_timestamp={run_timestamp})")

    try:
        # Determine if we're fetching all timestamps or just the latest
        if run_timestamp and run_timestamp.lower() == "all":
            # Query all entries across all timestamps
            query = f"""
            SELECT * FROM leaderboards.{leaderboard_name}
            ORDER BY run_timestamp DESC, rank ASC
            """
            params = {}
            results = execute_postgres_query(query, params)

            if not results:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for leaderboard '{leaderboard_name}'"
                )

            # Enrich with verified addresses (NOW OPTIMIZED - single query!)
            results = enrich_with_addresses(results)

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

            # Query all entries for the latest timestamp
            query = f"""
            SELECT * FROM leaderboards.{leaderboard_name}
            WHERE run_timestamp = :max_timestamp
            ORDER BY rank ASC
            """

            params = {"max_timestamp": max_timestamp}
            results = execute_postgres_query(query, params)

            if not results:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for leaderboard '{leaderboard_name}'"
                )

            # Enrich with verified addresses (NOW OPTIMIZED - single query!)
            results = enrich_with_addresses(results)

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
            # Query all entries for this user across all timestamps
            query = f"""
            SELECT * FROM leaderboards.{leaderboard_name}
            WHERE fid = :fid
            ORDER BY run_timestamp DESC
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

            # Enrich with verified addresses
            results = enrich_with_addresses(results)

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

            # Query the specific user's entry for the latest timestamp
            query = f"""
            SELECT * FROM leaderboards.{leaderboard_name}
            WHERE run_timestamp = :max_timestamp
            AND fid = :fid
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

            # Enrich with verified addresses
            result = enrich_with_addresses(results[0])

            logger.info(f"Retrieved entry for {user_identifier} from leaderboard '{leaderboard_name}'")

            return {
                "leaderboard_name": leaderboard_name,
                "user_identifier": user_identifier,
                "data": result,
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