# /app/api/endpoints/wallet_lookup.py
"""
Wallet lookup API endpoint - find wallets linked to a social account.
"""
import logging
from fastapi import APIRouter, HTTPException
from app.models.wallet_lookup_models import WalletLookupRequest, WalletLookupResponse
from app.db.neo4j import execute_cypher
from app.config import REPUTATION_PASS
from typing import Dict, Any

logger = logging.getLogger(__name__)
router = APIRouter()

PLATFORM_LABELS = {
    "x": "X",
    "farcaster": "WarpcastAccount"
}


@router.post(
    "/wallet-lookup",
    summary="Get wallets for a social account",
    description="Given a username and platform (farcaster or x), returns all linked Ethereum wallet addresses using graph traversal.",
    response_model=WalletLookupResponse,
    responses={
        200: {"description": "Successfully retrieved wallet addresses", "model": WalletLookupResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No wallets found for the provided username"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_wallets_for_username(request: WalletLookupRequest) -> Dict[str, Any]:
    """
    Get all linked wallet addresses for a social account.
    
    Uses APOC path expansion to traverse from social account through ACCOUNT 
    relationships to find all connected Wallet nodes.
    
    - Requires valid API key for authentication
    - Supports farcaster and x platforms
    - Returns only Ethereum addresses (0x prefixed)
    """
    logger.info(f"Looking up wallets for {request.platform}:{request.username}")
    
    if request.api_key != REPUTATION_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    label = PLATFORM_LABELS.get(request.platform)
    if not label:
        raise HTTPException(status_code=400, detail=f"Invalid platform: {request.platform}")
    
    username = request.username.lstrip('@').lower()
    
    try:
        query = f"""
        MATCH (account:{label} {{username: $username}})
        CALL apoc.path.expandConfig(account, {{
            relationshipFilter: "ACCOUNT",
            labelFilter: "/Wallet",
            minLevel: 1,
            maxLevel: 4
        }}) YIELD path
        WITH last(nodes(path)) as wallet
        WHERE wallet.address STARTS WITH '0x'
        RETURN DISTINCT wallet.address as address
        """
        
        results = execute_cypher(query, {"username": username})
        
        if not results:
            user_check = execute_cypher(
                f"MATCH (account:{label} {{username: $username}}) RETURN account.username as username",
                {"username": username}
            )
            
            if not user_check:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No {request.platform} account found with username: {username}"
                )
            
            logger.info(f"User {username} found but has no linked wallets")
            return {
                "username": username,
                "platform": request.platform,
                "addresses": [],
                "count": 0
            }
        
        addresses = [record.get("address") for record in results if record.get("address")]
        
        logger.info(f"Found {len(addresses)} wallet(s) for {request.platform}:{username}")
        
        return {
            "username": username,
            "platform": request.platform,
            "addresses": addresses,
            "count": len(addresses)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error looking up wallets for {request.platform}:{username}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")