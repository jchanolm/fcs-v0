# /app/api/endpoints/farcaster_connections.py
"""
Farcaster connections API endpoint - attention, influence, and mutuals.
"""
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from app.db.postgres import execute_postgres_query
from app.config import REPUTATION_PASS

logger = logging.getLogger(__name__)
router = APIRouter()


# --- Models ---

class ConnectionUser(BaseModel):
    """Base model for a connected user."""
    fid: int
    username: str
    pfp_url: Optional[str] = None
    rank: int
    score: int
    interaction_count: int
    is_mutual: bool = False


class MutualUser(BaseModel):
    """Model for a mutual connection."""
    fid: int
    username: str
    pfp_url: Optional[str] = None
    rank: int
    combined_score: int
    attention_score: int
    influence_score: int


class ConnectionsRequest(BaseModel):
    """Request model for farcaster connections endpoint."""
    fid: int = Field(..., description="Farcaster ID to get connections for")
    api_key: str = Field(..., description="API key for authentication")
    categories: Optional[str] = Field(
        None, 
        description="Comma-separated categories: attention, influence, mutuals. Empty for all."
    )


class ConnectionsResponse(BaseModel):
    """Response model for farcaster connections endpoint."""
    fid: int
    attention: Optional[List[ConnectionUser]] = None
    influence: Optional[List[ConnectionUser]] = None
    mutuals: Optional[List[MutualUser]] = None


# --- Queries ---

ATTENTION_QUERY = """
WITH attention_data AS (
    -- Likes (weight = 1)
    SELECT target_fid, 1 as weight
    FROM neynar.reactions
    WHERE reaction_type = 1
        AND fid = :fid
        AND target_fid != :fid
        AND timestamp >= NOW() - INTERVAL '1 month'
        AND deleted_at IS NULL
    UNION ALL
    -- Recasts (weight = 5)
    SELECT target_fid, 5 as weight
    FROM neynar.reactions
    WHERE reaction_type = 2
        AND fid = :fid
        AND target_fid != :fid
        AND timestamp >= NOW() - INTERVAL '1 month'
        AND deleted_at IS NULL
    UNION ALL
    -- Direct replies (weight = 5)
    SELECT parent_fid as target_fid, 5 as weight
    FROM neynar.casts
    WHERE parent_fid IS NOT NULL
        AND fid = :fid
        AND parent_fid != :fid
        AND timestamp >= NOW() - INTERVAL '1 month'
        AND deleted_at IS NULL
    UNION ALL
    -- Thread replies (weight = 3)
    SELECT r.fid AS target_fid, 3 as weight
    FROM neynar.casts c
    JOIN neynar.casts r ON c.root_parent_hash = r.hash
    WHERE c.root_parent_hash IS NOT NULL
        AND c.fid = :fid
        AND r.fid != :fid
        AND c.timestamp >= NOW() - INTERVAL '1 month'
        AND c.deleted_at IS NULL
    UNION ALL
    -- Mentions (weight = 5)
    SELECT mentioned_fid as target_fid, 5 as weight
    FROM neynar.recent_mentions
    WHERE source_fid = :fid
        AND mentioned_fid != :fid
)
SELECT
    ad.target_fid as fid,
    p.username,
    p.pfp_url,
    SUM(ad.weight)::int as score,
    COUNT(*)::int as interaction_count
FROM attention_data ad
LEFT JOIN neynar.profiles p ON ad.target_fid = p.fid
WHERE ad.target_fid IS NOT NULL
GROUP BY ad.target_fid, p.username, p.pfp_url
ORDER BY score DESC
LIMIT 25
"""

INFLUENCE_QUERY = """
WITH influence_data AS (
    -- Likes received (weight = 1)
    SELECT fid as source_fid, 1 as weight
    FROM neynar.reactions
    WHERE reaction_type = 1
        AND target_fid = :fid
        AND fid != :fid
        AND timestamp >= NOW() - INTERVAL '2 months'
        AND deleted_at IS NULL
    UNION ALL
    -- Recasts received (weight = 5)
    SELECT fid as source_fid, 5 as weight
    FROM neynar.reactions
    WHERE reaction_type = 2
        AND target_fid = :fid
        AND fid != :fid
        AND timestamp >= NOW() - INTERVAL '2 months'
        AND deleted_at IS NULL
    UNION ALL
    -- Direct replies received (weight = 5)
    SELECT fid as source_fid, 5 as weight
    FROM neynar.casts
    WHERE parent_fid = :fid
        AND fid != :fid
        AND timestamp >= NOW() - INTERVAL '2 months'
        AND deleted_at IS NULL
    UNION ALL
    -- Mentions received (weight = 5)
    SELECT source_fid, 5 as weight
    FROM neynar.recent_mentions
    WHERE mentioned_fid = :fid
        AND source_fid != :fid
)
SELECT
    id.source_fid as fid,
    p.username,
    p.pfp_url,
    SUM(id.weight)::int as score,
    COUNT(*)::int as interaction_count
FROM influence_data id
LEFT JOIN neynar.profiles p ON id.source_fid = p.fid
WHERE id.source_fid IS NOT NULL
GROUP BY id.source_fid, p.username, p.pfp_url
ORDER BY score DESC
LIMIT 25
"""


# --- Endpoint ---

@router.post(
    "/farcaster-connections",
    summary="Retrieve social connections)",
    description="""
    Retrieve social connection data for a Farcaster user.
    """,
    response_model=ConnectionsResponse,
    responses={
        200: {"description": "Successfully retrieved connections"},
        401: {"description": "Unauthorized - Invalid API key"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_farcaster_connections(request: ConnectionsRequest) -> Dict[str, Any]:
    """Get attention, influence, and/or mutual connections for a Farcaster user."""
    
    # Validate API key
    if request.api_key != REPUTATION_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    logger.info(f"Getting connections for FID {request.fid}, categories: {request.categories}")
    
    # Parse categories
    if request.categories:
        categories = [c.strip().lower() for c in request.categories.split(",")]
        valid_categories = {"attention", "influence", "mutuals"}
        categories = [c for c in categories if c in valid_categories]
        if not categories:
            categories = ["attention", "influence", "mutuals"]
    else:
        categories = ["attention", "influence", "mutuals"]
    
    # We need both attention and influence if mutuals is requested
    need_attention = "attention" in categories or "mutuals" in categories
    need_influence = "influence" in categories or "mutuals" in categories
    
    response: Dict[str, Any] = {"fid": request.fid}
    attention_map: Dict[int, Dict] = {}
    influence_map: Dict[int, Dict] = {}
    
    try:
        # Fetch attention data
        if need_attention:
            attention_results = execute_postgres_query(
                ATTENTION_QUERY, 
                {"fid": request.fid}
            )
            for idx, row in enumerate(attention_results):
                fid = row["fid"]
                attention_map[fid] = {
                    "fid": fid,
                    "username": row["username"] or "",
                    "pfp_url": row["pfp_url"],
                    "rank": idx + 1,
                    "score": row["score"],
                    "interaction_count": row["interaction_count"],
                    "is_mutual": False  # Will update after influence query
                }
        
        # Fetch influence data
        if need_influence:
            influence_results = execute_postgres_query(
                INFLUENCE_QUERY,
                {"fid": request.fid}
            )
            for idx, row in enumerate(influence_results):
                fid = row["fid"]
                influence_map[fid] = {
                    "fid": fid,
                    "username": row["username"] or "",
                    "pfp_url": row["pfp_url"],
                    "rank": idx + 1,
                    "score": row["score"],
                    "interaction_count": row["interaction_count"],
                    "is_mutual": False
                }
        
        # Find mutuals (intersection of attention and influence)
        mutual_fids = set(attention_map.keys()) & set(influence_map.keys())
        
        # Tag is_mutual on attention and influence results
        for fid in mutual_fids:
            if fid in attention_map:
                attention_map[fid]["is_mutual"] = True
            if fid in influence_map:
                influence_map[fid]["is_mutual"] = True
        
        # Build response based on requested categories
        if "attention" in categories:
            response["attention"] = list(attention_map.values())
        
        if "influence" in categories:
            response["influence"] = list(influence_map.values())
        
        if "mutuals" in categories:
            mutuals_list = []
            for fid in mutual_fids:
                att = attention_map[fid]
                inf = influence_map[fid]
                combined_score = att["score"] + inf["score"]
                mutuals_list.append({
                    "fid": fid,
                    "username": att["username"] or inf["username"],
                    "pfp_url": att["pfp_url"] or inf["pfp_url"],
                    "combined_score": combined_score,
                    "attention_score": att["score"],
                    "influence_score": inf["score"],
                    "rank": 0  # Will set after sorting
                })
            
            # Sort by combined_score DESC and assign ranks
            mutuals_list.sort(key=lambda x: x["combined_score"], reverse=True)
            for idx, mutual in enumerate(mutuals_list):
                mutual["rank"] = idx + 1
            
            response["mutuals"] = mutuals_list
        
        logger.info(
            f"Connections for FID {request.fid}: "
            f"attention={len(attention_map)}, influence={len(influence_map)}, mutuals={len(mutual_fids)}"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching connections for FID {request.fid}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")