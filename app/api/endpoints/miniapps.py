"""
Miniapp-related API endpoints.
"""
import logging
import httpx
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
from app.models.miniapp_models import (
    KeyPromotersRequest, KeyPromotersData
)
from app.db.neo4j import execute_cypher
from app.db.mongo import search_mongo_casts
from app.config import FARSTORE_PASS, NEYNAR_API_KEY
from typing import Dict, Any, List

# Set up logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

@router.post(
    "/farstore-miniapp-key-promoters", 
    summary="Get key promoters for a miniapp",
    description=(
        "Search Neynar and MongoDB for casts mentioning a mini‑app, merge the \n"
        "results, enrich each author with fcCredScore from Neo4j, and return \n"
        "the top 25 promoters ordered by credibility score."
    ),
)
async def retrieve_miniapp_key_promoters(
    request: KeyPromotersRequest,
    api_key: str = Query(..., description="API key for authentication"),
) -> Dict[str, Any]:
    # --------------- Auth -----------------
    if api_key != FARSTORE_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")

    miniapp_name = request.miniapp_name.strip()
    if not miniapp_name:
        raise HTTPException(status_code=400, detail="Miniapp name required")

    # --------------- Collect casts -----------------
    casts: List[Dict[str, Any]] = []

    # Neynar search
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(
                "https://api.neynar.com/v2/farcaster/cast/search",
                params={"q": miniapp_name, "limit": 100},
                headers={"accept": "application/json", "api_key": NEYNAR_API_KEY},
            )
        r.raise_for_status()
        for c in r.json().get("casts", []):
            author = c.get("author", {})
            casts.append(
                {
                    "hash": c.get("hash"),
                    "text": c.get("text", ""),
                    "timestamp": c.get("timestamp"),
                    "author_fid": author.get("fid"),
                    "author_username": author.get("username"),
                }
            )
        logger.info("Neynar returned %s casts", len(casts))
    except Exception as e:
        logger.error("Neynar search failed: %s", e)

    # Mongo search
    for m in await search_mongo_casts(miniapp_name, limit=100):
        ts = m.get("timestamp") or m.get("createdAt")
        if isinstance(ts, datetime):
            ts = ts.isoformat()
        casts.append(
            {
                "hash": m.get("hash"),
                "text": m.get("text", ""),
                "timestamp": ts,
                "author_fid": m.get("authorFid"),
                "author_username": m.get("author"),
            }
        )

    logger.info("Combined raw casts: %s", len(casts))

    # --------------- De‑duplicate by hash -----------------
    casts = {c["hash"]: c for c in casts if c.get("hash")}.values()

    # --------------- Enrich authors with Neo4j fcCredScore -----------------
    fids = sorted({int(c["author_fid"]) for c in casts if c.get("author_fid") is not None})
    if not fids:
        return {"promoters": []}

    records = execute_cypher(
        """
        MATCH (wc:Warpcast:Account)
        WHERE toInteger(wc.fid) IN $fids
        RETURN wc.fid AS fid,
               wc.username AS username,
               wc.fcCredScore AS fcCredScore,
               wc.bio AS bio
        """,
        {"fids": fids},
    )
    enrichment = {int(r["fid"]): dict(r) for r in records}
    if not enrichment:
        return {"promoters": []}

    # --------------- Build promoter objects -----------------
    promoters: List[Dict[str, Any]] = []
    for c in casts:
        fid = int(c.get("author_fid")) if c.get("author_fid") else None
        if fid is None or fid not in enrichment:
            continue

        # ensure we only capture up to 3 recent casts per promoter
        prom = next((p for p in promoters if p["fid"] == fid), None)
        if prom is None:
            prom = {
                "username": enrichment[fid]["username"],
                "fid": fid,
                "fcCredScore": enrichment[fid].get("fcCredScore") or 0,
                "bio": enrichment[fid].get("bio") or "",
                "recentCasts": [],
            }
            promoters.append(prom)

        if len(prom["recentCasts"]) < 3:
            prom["recentCasts"].append(
                {
                    "text": c["text"],
                    "hash": c["hash"],
                    "timestamp": c["timestamp"],
                }
            )

    # sort and limit
    promoters.sort(key=lambda x: x["fcCredScore"], reverse=True)
    return {"promoters": promoters[:25]}