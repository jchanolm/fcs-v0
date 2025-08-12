# app/api/endpoints/allowlist.py
"""
Allowlist API endpoints for FCS-v0.
"""
import logging
from fastapi import APIRouter, HTTPException
from app.models.allowlist_models import (
    AllowlistCheckRequest, AllowlistCheckResponse, AllowlistMode,
    CheckResponse, UsersResponse, UserEligibilityData, ConditionResult
)
from app.db.neo4j import execute_cypher
from typing import Dict, Any

# Set up logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.post(
    "/allowlist/check",
    summary="Check allowlist eligibility",
    description="Check user eligibility against an allowlist or retrieve all eligible users. API key required for authentication.",
    response_model=AllowlistCheckResponse,
    responses={
        200: {"description": "Successfully retrieved allowlist data", "model": AllowlistCheckResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "Allowlist not found or no eligible users"},
        400: {"description": "Bad Request - Invalid parameters"},
        500: {"description": "Internal Server Error"}
    }
)
async def check_allowlist_eligibility(request: AllowlistCheckRequest) -> Dict[str, Any]:
    """
    Check allowlist eligibility for users.
    
    Two modes available:
    - **users**: Returns all eligible users for the allowlist
    - **check**: Checks eligibility for a specific FID
    
    - Requires valid API key for authentication
    - Increments request count on each call
    - Returns detailed condition checking for individual users
    """
    # Validate API key
    
    logger.info(f"Processing allowlist {request.mode} request for query_id: {request.query_id}")
    
    try:
        # First, increment the request count and verify allowlist exists
        increment_query = """
        MATCH (allowlist:_Allowlist {uuid: $queryId})
        WHERE NOT allowlist:_Draft
        SET allowlist.requestCount = COALESCE(allowlist.requestCount, 0) + 1
        RETURN allowlist.requestCount as newCount
        """
        
        increment_result = execute_cypher(increment_query, {"queryId": request.query_id})
        
        if not increment_result:
            raise HTTPException(status_code=404, detail="Allowlist not found")
        
        new_request_count = increment_result[0].get("newCount", 1)
        logger.info(f"Incremented request count to {new_request_count} for allowlist {request.query_id}")
        
        if request.mode == AllowlistMode.users:
            # Get all eligible users
            users_data = await get_all_eligible_users(request.query_id)
            response_data = UsersResponse(
                users=users_data,
                total_count=len(users_data)
            )
        else:  # mode == "check"
            # Check specific user eligibility
            check_data = await check_user_eligibility(request.query_id, request.fid)
            response_data = check_data
        
        return AllowlistCheckResponse(
            query_id=request.query_id,
            mode=request.mode.value,
            request_count=new_request_count,
            data=response_data
        ).model_dump()
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error processing allowlist request: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

async def get_all_eligible_users(query_id: str) -> list[UserEligibilityData]:
    """Get all users eligible for the allowlist."""
    
    users_query = """
    MATCH (allowlist:_Allowlist {uuid: $allowlistId})
    WHERE NOT allowlist:_Draft

    // Find users who meet the reputation requirement  
    MATCH (user:WarpcastAccount)
    OPTIONAL MATCH (user)-[rr:ACCOUNT {primary: True}]->(wallet:Wallet {protocol: 'ethereum'})
    WHERE user.earlySummerNorm >= allowlist.fcCredCutoff

    // Get all conditions for this allowlist
    OPTIONAL MATCH (allowlist)-[cond:_ALLOWLIST_CONDITION]->(target)

    // Check if user has the required relationship to each target
    OPTIONAL MATCH (user)-[rel]-(target)
    WHERE (
      (cond.type = 'farcaster-follower' AND type(rel) = 'FOLLOWS') OR
      (cond.type = 'farcaster-channel' AND type(rel) IN ['MEMBER', 'FOLLOWS']) OR
      (cond.type = 'token-holder' AND EXISTS { MATCH (user)-[:ACCOUNT]->(wallet:Wallet)-[:HOLDS]->(target) }) OR
      (cond.type = 'miniapp-users' AND type(rel) = '_HAS_CONTEXT')
    )

    // Group by user and ensure they meet ALL conditions
    WITH user, 
         count(DISTINCT cond) as totalConditions,
         count(DISTINCT rel) as metConditions,
         wallet.address as primaryEthAddress

    // Only return users who meet ALL conditions (or no conditions exist)
    WHERE totalConditions = 0 OR totalConditions = metConditions

    RETURN 
      user.fid as fid,
      user.username as username,
      user.pfpUrl as pfpUrl,
      user.earlySummerNorm as quotientScore,
      user.earlySummerRank as quotientRank,
      primaryEthAddress
    ORDER BY user.earlySummerNorm DESC
    """
    
    result = execute_cypher(users_query, {"allowlistId": query_id})
    
    users = []
    for record in result:
        fid = record.get('fid')
        quotient_score = record.get('quotientScore')
        quotient_rank = record.get('quotientRank')
        primaryEthAddress = record.get('primaryEthAddress')
        
        # Handle Neo4j integer types
        fid_int = fid.toNumber() if hasattr(fid, 'toNumber') else int(fid)
        score_float = quotient_score.toNumber() if hasattr(quotient_score, 'toNumber') else float(quotient_score or 0)
        rank_int = quotient_rank.toNumber() if hasattr(quotient_rank, 'toNumber') else (int(quotient_rank) if quotient_rank else None)
        
        users.append(UserEligibilityData(
            fid=fid_int,
            username=record.get('username') or '',
            pfp_url=record.get('pfpUrl'),
            quotient_score=score_float,
            quotient_rank=rank_int,
            primary_eth_address=record.get('primaryEthAddress'),
            eligible=True  # All returned users are eligible
        ))
    
    logger.info(f"Found {len(users)} eligible users for allowlist {query_id}")
    return users

async def check_user_eligibility(query_id: str, fid: int) -> CheckResponse:
    """Check eligibility for a specific user."""
    
    check_query = """
Here's the complete fixed query for your Python API:
cypherMATCH (allowlist:_Allowlist {uuid: $allowlistId})
WHERE NOT allowlist:_Draft

MATCH (user:WarpcastAccount {fid: $fid})
OPTIONAL MATCH (user)-[rr:ACCOUNT {primary: True}]->(wallet:Wallet {protocol: 'ethereum'})

// Check reputation requirement
WITH allowlist, user, user.earlySummerNorm >= allowlist.fcCredCutoff as meetsReputation, wallet.address as primaryEthAddress

// Get all conditions with their targets
OPTIONAL MATCH (allowlist)-[cond:_ALLOWLIST_CONDITION]->(target)

// Check each condition type and only create condition objects when cond exists
WITH allowlist, user, meetsReputation, 
     [condition IN collect(CASE WHEN cond IS NOT NULL THEN {
       type: cond.type,
       targetName: CASE 
         WHEN target:WarpcastAccount THEN target.username
         WHEN target:Channel THEN target.channelId  
         WHEN target:Token THEN target.address
         WHEN target:_Context THEN 
           [(target)-[:_USAGE_CONTEXT]-(m:Miniapp) | m.name][0] + " - " + target._displayName
         ELSE "Unknown"
       END,
       meets: CASE cond.type
         WHEN 'farcaster-follower' THEN 
           EXISTS { MATCH (user)-[:FOLLOWS]->(target) }
         WHEN 'farcaster-channel' THEN 
           EXISTS { MATCH (user)-[:MEMBER|FOLLOWS]->(target) }
         WHEN 'token-holder' THEN 
           EXISTS { MATCH (user)-[:ACCOUNT]->(wallet:Wallet)-[:HOLDS]->(target) }
         WHEN 'miniapp-users' THEN
           EXISTS { MATCH (user)-[:_HAS_CONTEXT]->(target) }
         ELSE false
       END
     } END) WHERE condition IS NOT NULL] as conditions,
     primaryEthAddress

RETURN 
  user.fid as fid,
  user.earlySummerNorm as quotientScore,
  meetsReputation,
  conditions,
  meetsReputation AND size([c IN conditions WHERE c.meets = false]) = 0 as overallEligible    """
    
    result = execute_cypher(check_query, {"allowlistId": query_id, "fid": fid})
    
    if not result:
        raise HTTPException(status_code=404, detail="User not found or allowlist not found")
    
    record = result[0]
    
    # Handle Neo4j types
    fid_result = record.get('fid')
    quotient_score = record.get('quotientScore')
    fid_int = fid_result.toNumber() if hasattr(fid_result, 'toNumber') else int(fid_result)
    score_float = quotient_score.toNumber() if hasattr(quotient_score, 'toNumber') else float(quotient_score or 0)
    
    # Process conditions
    conditions = record.get('conditions') or []
    condition_results = []
    
    for condition in conditions:
        if condition.get('targetName'):  # Only include conditions with valid targets
            condition_results.append(ConditionResult(
                type=condition.get('type'),
                target_name=condition.get('targetName'),
                meets_condition=condition.get('meets', False)
            ))
    
    check_response = CheckResponse(
        fid=fid_int,
        eligible=record.get('overallEligible', False),
        quotient_score=score_float,
        meets_reputation_threshold=record.get('meetsReputation', False),
        conditions=condition_results
    )
    
    logger.info(f"Checked eligibility for FID {fid} on allowlist {query_id}: eligible={check_response.eligible}")
    return check_response