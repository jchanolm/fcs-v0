# /app/api/endpoints/wallet_lookup.py
"""
Wallet lookup API endpoint - find wallets linked to a Farcaster handle.
"""
import logging
from fastapi import APIRouter, HTTPException
from app.models.wallet_lookup_models import WalletLookupRequest, WalletLookupResponse
from app.db.neo4j import execute_cypher
from app.config import REPUTATION_PASS
from typing import Dict, Any

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post(
    "/wallet-lookup",
    summary="Get wallets for an X handle",
    description="Given an X (Twitter) handle, returns all linked Ethereum wallet addresses using graph traversal.",
    response_model=WalletLookupResponse,
    responses={
        200: {"description": "Successfully retrieved wallet addresses", "model": WalletLookupResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No wallets found for the provided handle"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_wallets_for_handle(request: WalletLookupRequest) -> Dict[str, Any]:
    """
    Get all linked wallet addresses for an X handle.
    
    Uses APOC path expansion to traverse from X account through ACCOUNT 
    relationships to find all connected Wallet nodes.
    
    - Requires valid API key for authentication
    - Returns only Ethereum addresses (0x prefixed)
    """
    logger.info(f"Looking up wallets for handle: {request.handle}")
    
    # Validate API key
    if request.api_key != REPUTATION_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Normalize handle (remove @ if present)
    handle = request.handle.lstrip('@').lower()
    
    try:
        # Use APOC path expandConfig to traverse from X account to Wallet nodes
        # /Wallet = termination filter (stop expansion at Wallet nodes)
        query = """
        MATCH (x:X {username: $handle})
        CALL apoc.path.expandConfig(x, {
            relationshipFilter: "ACCOUNT",
            labelFilter: "/Wallet",
            minLevel: 1,
            maxLevel: 4
        }) YIELD path
        WITH last(nodes(path)) as wallet
        WHERE wallet.address STARTS WITH '0x'
        RETURN DISTINCT wallet.address as address
        """
        
        results = execute_cypher(query, {"handle": handle})
        
        if not results:
            # Check if the user exists but has no wallets
            user_check = execute_cypher(
                "MATCH (x:X {handle: $handle}) RETURN x.handle as handle",
                {"handle": handle}
            )
            
            if not user_check:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No X account found with handle: {handle}"
                )
            
            # User exists but no wallets
            logger.info(f"User {handle} found but has no linked wallets")
            return {
                "handle": handle,
                "addresses": [],
                "count": 0
            }
        
        # Extract addresses from results
        addresses = [record.get("address") for record in results if record.get("address")]
        
        logger.info(f"Found {len(addresses)} wallet(s) for handle {handle}")
        
        return {
            "handle": handle,
            "addresses": addresses,
            "count": len(addresses)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error looking up wallets for handle {handle}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")