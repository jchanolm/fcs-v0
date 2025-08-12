# app/api/endpoints/allowlist.py
"""
Allowlist API endpoints for FCS-v0.
"""
import logging
from fastapi import APIRouter, HTTPException, Header
from app.models.allowlist_models import (
    UsersResponse, CheckResponse, UserEligibilityData, ConditionResult
)
from app.db.neo4j import execute_cypher
from typing import Optional

# Set up logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.get(
    "/allowlist/{query_id}/users",
    summary="Get all eligible users",
    description="Retrieve all users eligible for the specified allowlist",
    response_model=UsersResponse
)
async def get_eligible_users(
    query_id: str,
    api_key: str = Header(..., description="API key for authentication")
) -> UsersResponse:
    """Get all users eligible for the allowlist."""
    
    logger.info(f"Getting all eligible users for allowlist: {query_id}")
    
    # Increment request count
    increment_query = """
    MATCH (allowlist:_Allowlist {uuid: $queryId})
    WHERE NOT allowlist:_Draft
    SET allowlist._requestCount = COALESCE(allowlist._requestCount, 0) + 1
    RETURN allowlist._requestCount as requestCount
    """
    
    increment_result = execute_cypher(increment_query, {"queryId": query_id})
    if not increment_result:
        raise HTTPException(status_code=404, detail="Allowlist not found")
    
    # Get eligible users
    users_query = """
    MATCH (allowlist:_Allowlist {uuid: $allowlistId})
    WHERE NOT allowlist:_Draft

    // Find users who meet the reputation requirement  
    MATCH (user:WarpcastAccount)
    WHERE user.earlySummerNorm >= allowlist.fcCredCutoff
    
    // Get primary wallet
    OPTIONAL MATCH (user)-[r:ACCOUNT {primary: true}]->(wallet:Wallet {protocol: 'ethereum'})
    
    // Get all condition targets for this allowlist
    OPTIONAL MATCH (allowlist)-[cond:_ALLOWLIST_CONDITION]->(condTarget)
    WITH allowlist, user, wallet.address as primaryEthAddress,
         collect(DISTINCT condTarget) as conditionTargets
    
    // Check if user meets all conditions (or if there are no conditions)
    WHERE size(conditionTargets) = 0 OR 
          ALL(target IN conditionTargets WHERE
            CASE 
              WHEN target:WarpcastAccount THEN 
                EXISTS((user)-[:FOLLOWS]->(target))
              WHEN target:Channel THEN 
                EXISTS((user)-[:MEMBER|FOLLOWS]->(target))
              WHEN target:Token THEN 
                EXISTS((user)-[:ACCOUNT]->(:Wallet)-[:HOLDS]->(target))
              WHEN target:_Context THEN 
                EXISTS((user)-[:_HAS_CONTEXT]-(target))
              ELSE false
            END
          )
    
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
            eligible=True
        ))
    
    logger.info(f"Found {len(users)} eligible users for allowlist {query_id}")
    
    return UsersResponse(
        users=users,
        total_count=len(users),
        request_count=increment_result[0].get('requestCount')
    )


@router.get(
    "/allowlist/{query_id}/users/{fid}",
    summary="Check user eligibility",
    description="Check if a specific user (FID) is eligible for the allowlist",
    response_model=CheckResponse
)
async def check_user_eligibility(
    query_id: str,
    fid: int,
    api_key: str = Header(..., description="API key for authentication")
) -> CheckResponse:
    """Check eligibility for a specific user."""
    
    logger.info(f"Checking eligibility for FID {fid} on allowlist: {query_id}")
    
    # Increment request count
    increment_query = """
    MATCH (allowlist:_Allowlist {uuid: $queryId})
    WHERE NOT allowlist:_Draft
    SET allowlist._requestCount = COALESCE(allowlist._requestCount, 0) + 1
    RETURN allowlist._requestCount as requestCount
    """
    
    increment_result = execute_cypher(increment_query, {"queryId": query_id})
    if not increment_result:
        raise HTTPException(status_code=404, detail="Allowlist not found")
    
    # Check user eligibility
    check_query = """
    MATCH (allowlist:_Allowlist {uuid: $allowlistId})
    WHERE NOT allowlist:_Draft

    MATCH (user:WarpcastAccount {fid: $fid})
    OPTIONAL MATCH (user)-[rr:ACCOUNT {primary: true}]->(wallet:Wallet {protocol: 'ethereum'})

    // Check reputation requirement
    WITH allowlist, user, user.earlySummerNorm >= allowlist.fcCredCutoff as meetsReputation, wallet.address as primaryEthAddress

    // Get all conditions with their targets
    OPTIONAL MATCH (allowlist)-[cond:_ALLOWLIST_CONDITION]->(target)

    // Check each condition type
    WITH allowlist, user, meetsReputation, primaryEthAddress,
         [condition IN collect(CASE WHEN cond IS NOT NULL THEN {
           type: cond.type,
           targetName: CASE 
             WHEN target:WarpcastAccount THEN target.username
             WHEN target:Channel THEN target.channelId  
             WHEN target:Token THEN target.address
             WHEN target:_Context THEN 
               [(target)<-[:_USAGE_CONTEXT]-(m:Miniapp) | m.name][0] + " - " + target._displayName
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
               EXISTS { MATCH (user)-[:_HAS_CONTEXT]-(target) }
             ELSE false
           END
         } END) WHERE condition IS NOT NULL] as conditions

    RETURN 
      user.fid as fid,
      user.username as username,
      user.earlySummerNorm as quotientScore,
      meetsReputation,
      conditions,
      primaryEthAddress,
      meetsReputation AND size([c IN conditions WHERE c.meets = false]) = 0 as overallEligible
    """
    
    result = execute_cypher(check_query, {"allowlistId": query_id, "fid": fid})
    
    if not result:
        raise HTTPException(status_code=404, detail="User not found")
    
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
        if condition.get('targetName'):
            condition_results.append(ConditionResult(
                type=condition.get('type'),
                target_name=condition.get('targetName'),
                meets_condition=condition.get('meets', False)
            ))
    
    logger.info(f"User {fid} eligibility: {record.get('overallEligible')}")
    
    return CheckResponse(
        fid=fid_int,
        username=record.get('username'),
        eligible=record.get('overallEligible', False),
        quotient_score=score_float,
        meets_reputation_threshold=record.get('meetsReputation', False),
        conditions=condition_results,
        primary_eth_address=record.get('primaryEthAddress'),
        request_count=increment_result[0].get('requestCount')
    )