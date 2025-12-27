# /app/api/endpoints/farcaster_connections_all.py
"""
Farcaster connections endpoint - all mutuals with affinity scoring.
"""
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from app.db.postgres import execute_postgres_query
from app.config import REPUTATION_PASS

logger = logging.getLogger(__name__)
router = APIRouter()


class MutualUser(BaseModel):
    """Model for a mutual connection with affinity scores."""
    fid: int
    username: str
    pfp_url: Optional[str] = None
    rank: int
    combined_score: float
    attention_score: float
    influence_score: float


class ConnectionsAllRequest(BaseModel):
    """Request model for all mutuals endpoint."""
    fid: int = Field(..., description="Farcaster ID to get mutuals for")
    api_key: str = Field(..., description="API key for authentication")


class ConnectionsAllResponse(BaseModel):
    """Response model for all mutuals endpoint."""
    fid: int
    mutuals: List[MutualUser]
    count: int


MUTUALS_RANKED_QUERY = """
WITH mutuals AS (
    SELECT DISTINCT t1.target_fid AS fid
    FROM neynar.follows t1
    JOIN neynar.follows t2 ON t2.fid = t1.target_fid AND t2.target_fid = :fid
    WHERE t1.fid = :fid AND t1.target_fid <> :fid
    AND t1.deleted_at IS NULL 
    AND t2.deleted_at IS NULL
),
attention_likes AS (
    SELECT r.target_fid AS fid, COUNT(*) AS cnt
    FROM neynar.reactions r
    WHERE r.reaction_type = 1 AND r.fid = :fid
        AND r.timestamp >= CURRENT_DATE - INTERVAL '6 months'
        AND r.deleted_at IS NULL
        AND r.target_fid IN (SELECT fid FROM mutuals)
    GROUP BY r.target_fid
),
attention_recasts AS (
    SELECT r.target_fid AS fid, COUNT(*) AS cnt
    FROM neynar.reactions r
    WHERE r.reaction_type = 2 AND r.fid = :fid
        AND r.timestamp >= CURRENT_DATE - INTERVAL '6 months'
        AND r.deleted_at IS NULL
        AND r.target_fid IN (SELECT fid FROM mutuals)
    GROUP BY r.target_fid
),
attention_replies AS (
    SELECT c.parent_fid AS fid, COUNT(*) AS cnt
    FROM neynar.casts c
    WHERE c.parent_fid IS NOT NULL AND c.fid = :fid
        AND c.timestamp >= CURRENT_DATE - INTERVAL '6 months'
        AND c.deleted_at IS NULL
        AND c.parent_fid IN (SELECT fid FROM mutuals)
    GROUP BY c.parent_fid
),
attention_threads AS (
    SELECT r2.fid AS fid, COUNT(*) AS cnt
    FROM neynar.casts c2
    JOIN neynar.casts r2 ON c2.root_parent_hash = r2.hash
    WHERE c2.root_parent_hash IS NOT NULL AND c2.fid = :fid
        AND c2.timestamp >= CURRENT_DATE - INTERVAL '6 months'
        AND c2.deleted_at IS NULL
        AND r2.fid IN (SELECT fid FROM mutuals)
    GROUP BY r2.fid
),
attention_mentions AS (
    SELECT rm.mentioned_fid AS fid, COUNT(*) AS cnt
    FROM neynar.recent_mentions rm
    WHERE rm.source_fid = :fid
        AND rm.mentioned_fid IN (SELECT fid FROM mutuals)
    GROUP BY rm.mentioned_fid
),
influence_likes AS (
    SELECT r.fid AS fid, COUNT(*) AS cnt
    FROM neynar.reactions r
    WHERE r.reaction_type = 1 AND r.target_fid = :fid
        AND r.timestamp >= CURRENT_DATE - INTERVAL '6 months'
        AND r.deleted_at IS NULL
        AND r.fid IN (SELECT fid FROM mutuals)
    GROUP BY r.fid
),
influence_recasts AS (
    SELECT r.fid AS fid, COUNT(*) AS cnt
    FROM neynar.reactions r
    WHERE r.reaction_type = 2 AND r.target_fid = :fid
        AND r.timestamp >= CURRENT_DATE - INTERVAL '6 months'
        AND r.deleted_at IS NULL
        AND r.fid IN (SELECT fid FROM mutuals)
    GROUP BY r.fid
),
influence_replies AS (
    SELECT c.fid AS fid, COUNT(*) AS cnt
    FROM neynar.casts c
    WHERE c.parent_fid = :fid
        AND c.timestamp >= CURRENT_DATE - INTERVAL '6 months'
        AND c.deleted_at IS NULL
        AND c.fid IN (SELECT fid FROM mutuals)
    GROUP BY c.fid
),
influence_mentions AS (
    SELECT rm.source_fid AS fid, COUNT(*) AS cnt
    FROM neynar.recent_mentions rm
    WHERE rm.mentioned_fid = :fid
        AND rm.source_fid IN (SELECT fid FROM mutuals)
    GROUP BY rm.source_fid
),
scored AS (
    SELECT
        m.fid,
        COALESCE(p.username, '') AS username,
        COALESCE(p.pfp_url, '') AS pfp_url,
        (COALESCE(al.cnt, 0) * 1 + COALESCE(ar.cnt, 0) * 5 + COALESCE(arep.cnt, 0) * 5 + COALESCE(at.cnt, 0) * 3 + COALESCE(am.cnt, 0) * 5) AS attention_score,
        (COALESCE(il.cnt, 0) * 1 + COALESCE(ir.cnt, 0) * 5 + COALESCE(irep.cnt, 0) * 5 + COALESCE(im.cnt, 0) * 5) AS influence_score
    FROM mutuals m
    LEFT JOIN neynar.profiles p ON p.fid = m.fid
    LEFT JOIN attention_likes al ON al.fid = m.fid
    LEFT JOIN attention_recasts ar ON ar.fid = m.fid
    LEFT JOIN attention_replies arep ON arep.fid = m.fid
    LEFT JOIN attention_threads at ON at.fid = m.fid
    LEFT JOIN attention_mentions am ON am.fid = m.fid
    LEFT JOIN influence_likes il ON il.fid = m.fid
    LEFT JOIN influence_recasts ir ON ir.fid = m.fid
    LEFT JOIN influence_replies irep ON irep.fid = m.fid
    LEFT JOIN influence_mentions im ON im.fid = m.fid
)
SELECT
    fid,
    username,
    pfp_url,
    attention_score,
    influence_score,
    (attention_score + influence_score) AS combined_score
FROM scored
ORDER BY combined_score DESC, username ASC
"""


@router.post(
    "/farcaster-connections-all",
    summary="Get all mutuals with affinity ranking",
    description="Retrieve all mutual connections for a Farcaster user, ranked by combined attention + influence scores.",
    response_model=ConnectionsAllResponse,
    responses={
        200: {"description": "Successfully retrieved ranked mutuals"},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No mutuals found for the provided FID"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_all_mutuals_ranked(request: ConnectionsAllRequest) -> Dict[str, Any]:
    """Get all mutual connections ranked by affinity score."""
    
    if request.api_key != REPUTATION_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    logger.info(f"Getting all ranked mutuals for FID {request.fid}")
    
    try:
        results = execute_postgres_query(MUTUALS_RANKED_QUERY, {"fid": request.fid})
        
        if not results:
            raise HTTPException(status_code=404, detail=f"No mutuals found for FID {request.fid}")
        
        mutuals = []
        for idx, row in enumerate(results):
            mutuals.append({
                "fid": row["fid"],
                "username": row["username"] or "",
                "pfp_url": row["pfp_url"],
                "rank": idx + 1,
                "combined_score": float(row["combined_score"] or 0),
                "attention_score": float(row["attention_score"] or 0),
                "influence_score": float(row["influence_score"] or 0)
            })
        
        logger.info(f"Returning {len(mutuals)} ranked mutuals for FID {request.fid}")
        
        return {
            "fid": request.fid,
            "mutuals": mutuals,
            "count": len(mutuals)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching mutuals for FID {request.fid}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")