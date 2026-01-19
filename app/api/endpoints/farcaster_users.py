# /app/api/endpoints/farcaster_users.py
"""
Farcaster users API endpoints.
"""
import logging
from fastapi import APIRouter, HTTPException
from app.models.farcaster_models import (
    MutualsResponse, MutualsRequest, UserProfile,
    LinkedWalletsRequest, LinkedWalletsResponse
)
from app.db.postgres import execute_postgres_query
from app.config import REPUTATION_PASS
from typing import Dict, Any

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
    """
    logger.info(f"=== MUTUALS REQUEST START ===")
    logger.info(f"FID: {request.fid} (type: {type(request.fid)})")
    
    # Validate API key
    if request.api_key != REPUTATION_PASS:
        logger.error(f"Invalid API key provided")
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    try:
        # The actual query that should work
        query = """
        SELECT DISTINCT
            t1.target_fid as fid,
            COALESCE(p.username, '') as username,
            COALESCE(p.pfp_url, '') as pfp_url
        FROM neynar.follows t1
        INNER JOIN neynar.follows t2 ON (t2.fid = t1.target_fid AND t2.target_fid = :fid)
        LEFT JOIN neynar.profiles p ON p.fid = t1.target_fid
        WHERE t1.fid = :fid
        ORDER BY username
        """
        
        params = {"fid": int(request.fid)}
        
        logger.info(f"EXECUTING QUERY:")
        logger.info(f"Query: {query}")
        logger.info(f"Params: {params}")
        
        # Execute the query
        results = execute_postgres_query(query, params)
        
        logger.info(f"RAW POSTGRES RESPONSE:")
        logger.info(f"Type: {type(results)}")
        logger.info(f"Length: {len(results) if results else 'None'}")
        logger.info(f"Content: {results}")
        
        # Process results
        mutual_followers = []
        if results:
            logger.info(f"Processing {len(results)} records...")
            for i, record in enumerate(results):
                logger.info(f"Record {i}: {record} (type: {type(record)})")
                user_profile = UserProfile(
                    fid=record["fid"],
                    username=record["username"],
                    pfp_url=record["pfp_url"]
                )
                mutual_followers.append(user_profile)
                logger.info(f"Created user profile: {user_profile}")
        else:
            logger.warning(f"No results returned from PostgreSQL")
        
        logger.info(f"=== MUTUALS REQUEST END ===")
        logger.info(f"Returning {len(mutual_followers)} mutual followers")
        
        # Return the response
        return {
            "fid": request.fid,
            "mutual_followers": mutual_followers,
            "count": len(mutual_followers)
        }
        
    except Exception as e:
        logger.error(f"=== ERROR IN MUTUALS ENDPOINT ===")
        logger.error(f"Exception type: {type(e)}")
        logger.error(f"Exception message: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post(
    "/farcaster-linked-wallets",
    summary="Get all linked wallets for a wallet address",
    description="Given an Ethereum wallet address, returns the Farcaster user (if any) and all their verified wallet addresses. API key required for authentication.",
    response_model=LinkedWalletsResponse,
    responses={
        200: {"description": "Successfully retrieved linked wallets", "model": LinkedWalletsResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No Farcaster account found for the provided wallet address"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_linked_wallets(request: LinkedWalletsRequest) -> Dict[str, Any]:
    """
    Get all Farcaster-linked wallets for a given wallet address.
    
    Given an Ethereum wallet address, this endpoint:
    1. Looks up the Farcaster ID (FID) associated with that wallet
    2. Returns all verified wallet addresses linked to that Farcaster account
    
    - Requires valid API key for authentication
    - Returns the FID, username, and all linked wallet addresses
    - Wallet addresses are normalized to lowercase with 0x prefix
    """
    logger.info(f"=== LINKED WALLETS REQUEST START ===")
    logger.info(f"Wallet address: {request.wallet_address}")
    
    # Validate API key
    if request.api_key != REPUTATION_PASS:
        logger.error("Invalid API key provided")
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Normalize the input address
    normalized_address = request.wallet_address.lower()
    if not normalized_address.startswith("0x"):
        normalized_address = "0x" + normalized_address
    
    try:
        # Step 1: Look up the FID from the wallet address
        fid_query = """
        SELECT fid 
        FROM neynar.verifications 
        WHERE LOWER(CONCAT('0x', encode(address, 'hex'))) = LOWER(:wallet_address)
        LIMIT 1
        """
        
        fid_result = execute_postgres_query(fid_query, {"wallet_address": normalized_address})
        
        if not fid_result:
            logger.info(f"No FID found for wallet address: {normalized_address}")
            raise HTTPException(
                status_code=404, 
                detail=f"No Farcaster account found for wallet address: {normalized_address}"
            )
        
        fid = fid_result[0]["fid"]
        logger.info(f"Found FID {fid} for wallet {normalized_address}")
        
        # Step 2: Get username and all linked wallets for this FID
        wallets_query = """
        SELECT 
            p.username,
            COALESCE(
                ARRAY_AGG(DISTINCT CONCAT('0x', encode(v.address, 'hex'))),
                ARRAY[]::text[]
            ) as addresses
        FROM neynar.profiles p
        LEFT JOIN neynar.verifications v ON v.fid = p.fid
        WHERE p.fid = :fid
        GROUP BY p.username
        """
        
        wallets_result = execute_postgres_query(wallets_query, {"fid": fid})
        
        username = None
        linked_wallets = []
        
        if wallets_result:
            username = wallets_result[0].get("username")
            linked_wallets = wallets_result[0].get("addresses", [])
            # Filter out any None or empty values
            linked_wallets = [w.lower() for w in linked_wallets if w]
        
        logger.info(f"Found {len(linked_wallets)} linked wallets for FID {fid}")
        logger.info(f"=== LINKED WALLETS REQUEST END ===")
        
        return {
            "input_address": normalized_address,
            "fid": fid,
            "username": username,
            "linked_wallets": linked_wallets,
            "count": len(linked_wallets)
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"=== ERROR IN LINKED WALLETS ENDPOINT ===")
        logger.error(f"Exception type: {type(e)}")
        logger.error(f"Exception message: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")