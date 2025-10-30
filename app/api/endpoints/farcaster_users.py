# /app/api/endpoints/farcaster_users.py
"""
Farcaster users API endpoints.
"""
import logging
from fastapi import APIRouter, HTTPException
from app.models.farcaster_models import MutualsResponse, MutualsRequest, UserProfile
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


#         WITH mutuals AS (
#   SELECT DISTINCT t1.target_fid AS fid
#   FROM neynar.follows t1
#   JOIN neynar.follows t2 ON t2.fid = t1.target_fid AND t2.target_fid = $1
#   WHERE t1.fid = $1 AND t1.target_fid <> $1
# ),
# attention_likes AS (
#   SELECT r.target_fid AS fid, COUNT(*) AS cnt
#   FROM neynar.reactions r
#   WHERE r.reaction_type = 1 AND r.fid = $1
#     AND r.timestamp >= CURRENT_DATE - INTERVAL '6 months'
#     AND r.deleted_at IS NULL
#     AND r.target_fid IN (SELECT fid FROM mutuals)
#   GROUP BY r.target_fid
# ),
# attention_recasts AS (
#   SELECT r.target_fid AS fid, COUNT(*) AS cnt
#   FROM neynar.reactions r
#   WHERE r.reaction_type = 2 AND r.fid = $1
#     AND r.timestamp >= CURRENT_DATE - INTERVAL '6 months'
#     AND r.deleted_at IS NULL
#     AND r.target_fid IN (SELECT fid FROM mutuals)
#   GROUP BY r.target_fid
# ),
# attention_replies AS (
#   SELECT c.parent_fid AS fid, COUNT(*) AS cnt
#   FROM neynar.casts c
#   WHERE c.parent_fid IS NOT NULL AND c.fid = $1
#     AND c.timestamp >= CURRENT_DATE - INTERVAL '6 months'
#     AND c.deleted_at IS NULL
#     AND c.parent_fid IN (SELECT fid FROM mutuals)
#   GROUP BY c.parent_fid
# ),
# attention_threads AS (
#   SELECT r2.fid AS fid, COUNT(*) AS cnt
#   FROM neynar.casts c2
#   JOIN neynar.casts r2 ON c2.root_parent_hash = r2.hash
#   WHERE c2.root_parent_hash IS NOT NULL AND c2.fid = $1
#     AND c2.timestamp >= CURRENT_DATE - INTERVAL '6 months'
#     AND c2.deleted_at IS NULL
#     AND r2.fid IN (SELECT fid FROM mutuals)
#   GROUP BY r2.fid
# ),
# attention_mentions AS (
#   SELECT rm.mentioned_fid AS fid, COUNT(*) AS cnt
#   FROM neynar.recent_mentions rm
#   WHERE rm.source_fid = $1
#     AND rm.mentioned_fid IN (SELECT fid FROM mutuals)
#   GROUP BY rm.mentioned_fid
# ),
# influence_likes AS (
#   SELECT r.fid AS fid, COUNT(*) AS cnt
#   FROM neynar.reactions r
#   WHERE r.reaction_type = 1 AND r.target_fid = $1
#     AND r.timestamp >= CURRENT_DATE - INTERVAL '6 months'
#     AND r.deleted_at IS NULL
#     AND r.fid IN (SELECT fid FROM mutuals)
#   GROUP BY r.fid
# ),
# influence_recasts AS (
#   SELECT r.fid AS fid, COUNT(*) AS cnt
#   FROM neynar.reactions r
#   WHERE r.reaction_type = 2 AND r.target_fid = $1
#     AND r.timestamp >= CURRENT_DATE - INTERVAL '6 months'
#     AND r.deleted_at IS NULL
#     AND r.fid IN (SELECT fid FROM mutuals)
#   GROUP BY r.fid
# ),
# influence_replies AS (
#   SELECT c.fid AS fid, COUNT(*) AS cnt
#   FROM neynar.casts c
#   WHERE c.parent_fid = $1
#     AND c.timestamp >= CURRENT_DATE - INTERVAL '6 months'
#     AND c.deleted_at IS NULL
#     AND c.fid IN (SELECT fid FROM mutuals)
#   GROUP BY c.fid
# ),
# influence_mentions AS (
#   SELECT rm.source_fid AS fid, COUNT(*) AS cnt
#   FROM neynar.recent_mentions rm
#   WHERE rm.mentioned_fid = $1
#     AND rm.source_fid IN (SELECT fid FROM mutuals)
#   GROUP BY rm.source_fid
# ),
# scored AS (
#   SELECT
#     m.fid,
#     COALESCE(p.username, '') AS username,
#     COALESCE(p.pfp_url, '') AS pfp_url,
#     (COALESCE(al.cnt, 0) * 1 + COALESCE(ar.cnt, 0) * 5 + COALESCE(arep.cnt, 0) * 5 + COALESCE(at.cnt, 0) * 3 + COALESCE(am.cnt, 0) * 5) AS attention_score,
#     (COALESCE(al.cnt, 0) + COALESCE(ar.cnt, 0) + COALESCE(arep.cnt, 0) + COALESCE(at.cnt, 0) + COALESCE(am.cnt, 0)) AS attention_events,
#     (COALESCE(il.cnt, 0) * 1 + COALESCE(ir.cnt, 0) * 5 + COALESCE(irep.cnt, 0) * 5 + COALESCE(im.cnt, 0) * 5) AS influence_score,
#     (COALESCE(il.cnt, 0) + COALESCE(ir.cnt, 0) + COALESCE(irep.cnt, 0) + COALESCE(im.cnt, 0)) AS influence_events
#   FROM mutuals m
#   LEFT JOIN neynar.profiles p ON p.fid = m.fid
#   LEFT JOIN attention_likes al ON al.fid = m.fid
#   LEFT JOIN attention_recasts ar ON ar.fid = m.fid
#   LEFT JOIN attention_replies arep ON arep.fid = m.fid
#   LEFT JOIN attention_threads at ON at.fid = m.fid
#   LEFT JOIN attention_mentions am ON am.fid = m.fid
#   LEFT JOIN influence_likes il ON il.fid = m.fid
#   LEFT JOIN influence_recasts ir ON ir.fid = m.fid
#   LEFT JOIN influence_replies irep ON irep.fid = m.fid
#   LEFT JOIN influence_mentions im ON im.fid = m.fid
# )
# SELECT
#   fid,
#   username,
#   pfp_url,
#   attention_score,
#   influence_score,
#   attention_events,
#   influence_events,
#   CASE WHEN MAX(attention_score) OVER () > 0 THEN attention_score::numeric / MAX(attention_score) OVER () ELSE 0 END AS attention_norm,
#   CASE WHEN MAX(influence_score) OVER () > 0 THEN influence_score::numeric / MAX(influence_score) OVER () ELSE 0 END AS influence_norm,
#   ROUND(0.4 * (CASE WHEN MAX(attention_score) OVER () > 0 THEN attention_score::numeric / MAX(attention_score) OVER () ELSE 0 END) + 0.6 * (CASE WHEN MAX(influence_score) OVER () > 0 THEN influence_score::numeric / MAX(influence_score) OVER () ELSE 0 END), 6) AS affinity_score
# FROM scored
# ORDER BY affinity_score DESC, username ASC
