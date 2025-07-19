"""
Cast search API endpoints.
"""
import os
import logging
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
from app.models.cast_models import (
    CastRequest, WeightedCastsResponseData
)
from app.db.neo4j import execute_cypher
from app.utils.helpers import clean_query_for_lucene, save_search_results_to_json
from app.config import FART_PASS
from typing import Dict, Any, List

# Set up logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

async def search_casts(query: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Search for casts matching a query using MongoDB Atlas Search
    
    Args:
        query: Search query string
        limit: Maximum number of results to return
        
    Returns:
        List of matching cast documents
    """
    try:
        results = await search_mongo_casts(query, limit=limit)
        return results
    except Exception as e:
        logger.error(f"Error searching casts: {str(e)}")
        return []

@router.post(
    "/casts-search-weighted",
    summary="Search for casts with weighted scoring",
    description="Search for casts matching a query with weighted scoring based on author credibility. API key required for authentication.",
    responses={
        200: {"description": "Successfully retrieved weighted casts"},
        401: {"description": "Unauthorized - Invalid API key"},
        429: {"description": "Too Many Requests - Usage quota exceeded"},
        500: {"description": "Internal Server Error"}
    },
    openapi_extra={
        "parameters": [
            {
                "name": "api_key",
                "in": "query",
                "required": True,
                "schema": {"type": "string"},
                "description": "API key for authentication"
            }
        ]
    }
)
async def fetch_weighted_casts(
    request: CastRequest,
    api_key: str = Query(..., description="API key for authentication", example="fafakjfakjfa.lol")
) -> Dict[str, Any]:
    """
    Get matching casts and related metadata using a hybrid MongoDB Atlas Search + Neo4j approach.
    Returns all matching results without pagination.
    
    - Requires valid API key for authentication
    """
    # Validate API key
    if api_key != FART_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    try:
        # Check API usage limits
        usage_query = """
        MATCH (node:ApiUsage {api_key: "arbitrage.lol"})
        SET node.queryCounter = COALESCE(node.queryCounter, 0) + 1
        RETURN node.queryCounter as counter
        """
        
        usage_result = execute_cypher(usage_query, {})
        if usage_result and usage_result[0].get("counter", 0) > 250:
            logger.warning(f"API usage exceeded for arbitrage.lol: {usage_result[0].get('counter')} queries")
            raise HTTPException(status_code=429, detail="USAGE EXCEEDED")
        
        logger.info(f"Starting weighted casts search with query: '{request.query}'")
        start_time = datetime.now()
        
        # Define combined_casts early to avoid the issue
        combined_casts = []
        
        # ---------------------------------------------------------------------
        # 0) Clean the user's query for Neo4j fulltext and MongoDB Atlas Search
        # ---------------------------------------------------------------------
        clean_query = clean_query_for_lucene(request.query)
        logger.info(f"User's raw search: '{request.query}', cleaned for search: '{clean_query}'")
        
        # ---------------------------------------------------------------------
        # 1) Fetch from MongoDB Atlas Search if available
        # ---------------------------------------------------------------------
        mongo_start_time = datetime.now()
        mongo_casts_results = await search_casts(clean_query, limit=100)
        mongo_end_time = datetime.now()
        mongo_duration = (mongo_end_time - mongo_start_time).total_seconds()
        
        mongo_casts = []
        if mongo_casts_results:
            logger.info(f"MongoDB Atlas Search completed in {mongo_duration:.2f} seconds, returned {len(mongo_casts_results)} results")
            
            # Process MongoDB results into a consistent format
            for cast in mongo_casts_results:
                mongo_casts.append({
                    "hash": cast.get("hash"),
                    "timestamp": cast.get("timestamp") or cast.get("createdAt", ""),
                    "text": cast.get("text", ""),
                    "author_username": cast.get("author", ""),
                    "author_fid": cast.get("authorFid"),
                    "author_bio": "",  # Will be enriched from Neo4j
                    "likeCount": cast.get("likeCount", 0),
                    "replyCount": cast.get("replyCount", 0),
                    "mentionedChannels": cast.get("mentionedChannelIds", []),
                    "mentionedUsers": cast.get("mentionedUsernames", []),
                    "relevanceScore": cast.get("score", 0)
                })
        else:
            logger.info(f"MongoDB Atlas Search returned no results or is not available")
        
        # Log a sample of the MongoDB results
        if mongo_casts:
            sample_size = min(5, len(mongo_casts))
            logger.info(f"Sample of {sample_size} MongoDB casts:")
            for i, cast in enumerate(mongo_casts[:sample_size]):
                logger.info(f"  Cast {i+1}: hash={cast.get('hash')}, author={cast.get('author_username')}, timestamp={cast.get('timestamp')}")
                logger.info(f"    Text preview: {cast.get('text')[:50]}...")
        
        # ---------------------------------------------------------------------
        # 2) Combine + De-duplicate (by cast hash)
        # ---------------------------------------------------------------------
        # Instead of looking up by hash, we'll look up by FID to get author information
        
        # Collect all unique FIDs from MongoDB results
        mongo_fids = [str(cast.get("author_fid")) for cast in mongo_casts if cast.get("author_fid")]
        all_fids = list(set(mongo_fids))  # Remove duplicates
        
        logger.info(f"Looking up {len(all_fids)} unique FIDs in Neo4j for account enrichment")
        
        # FID-based author enrichment query
        enrichment_start_time = datetime.now()
        fid_enrichment_query = """
        MATCH (wc:Warpcast:Account)
        WHERE tointeger(wc.fid) IN $fids
        OPTIONAL MATCH (wc)-[:ACCOUNT]-(wallet:Wallet)
        OPTIONAL MATCH (wc)-[:ACCOUNT]-(account:Account)
        OPTIONAL MATCH ()-[rewards:REWARDS]->(:Wallet)-[:ACCOUNT]-(wc:Warpcast:Account)
        WITH 
            wc.fid as fid,
            wc.username as authorUsername,
            wc.bio as authorBio,
            wc.fcCredScore as fcCredScore,
            tofloat(sum(coalesce(tofloat(wallet.balance), 0))) as walletEthStablesValueUsd,
            tofloat(sum(coalesce(tofloat(rewards.value), 0))) as farcaster_usdc_rewards_earned,
            collect(distinct({platform: account.platform, username: account.username})) as linkedAccounts,
            collect(distinct({address: wallet.address, network: wallet.network})) as linkedWallets
        RETURN 
            fid,
            authorUsername,
            authorBio,
            fcCredScore,
            walletEthStablesValueUsd,
            farcaster_usdc_rewards_earned,
            [acc IN linkedAccounts WHERE acc.platform <> "Wallet"] as linkedAccounts,
            linkedWallets
        """
        
        # Execute the FID-based enrichment query
        enrichment_results = []
        if all_fids:
            # Run the Neo4j query test to verify connection
            try:
                test_result = execute_cypher("RETURN 1 as test", {})
                logger.info(f"Neo4j test query result: {test_result}")
                
                # Execute the actual enrichment query
                enrichment_results = execute_cypher(fid_enrichment_query, {"fids": all_fids})
            except Exception as ne:
                logger.error(f"Neo4j query failed: {str(ne)}")
                enrichment_results = []
        
        # Build FID -> enrichment data map
        fid_enrichment_map = {}
        for record in enrichment_results:
            fid = record.get("fid")
            if fid:
                fid_enrichment_map[fid] = {
                    "authorUsername": record.get("authorUsername"),
                    "authorBio": record.get("authorBio"),
                    "fcCredScore": record.get("fcCredScore"),
                    "walletEthStablesValueUsd": record.get("walletEthStablesValueUsd"),
                    "farcaster_usdc_rewards_earned": record.get("farcaster_usdc_rewards_earned"),
                    "linkedAccounts": record.get("linkedAccounts", []),
                    "linkedWallets": record.get("linkedWallets", []),
                }
        
        enrichment_end_time = datetime.now()
        enrichment_duration = (enrichment_end_time - enrichment_start_time).total_seconds()
        logger.info(f"FID enrichment query completed in {enrichment_duration:.2f} seconds, returned data for {len(fid_enrichment_map)} FIDs")
        
        # Now, enrich all casts with the FID data
        enriched_mongo_casts = []
        for cast in mongo_casts:
            fid = str(cast.get("author_fid"))
            
            # Create a structured cast with all required fields
            enriched_cast = {
                "hash": cast.get("hash"),
                "timestamp": cast.get("timestamp"),
                "text": cast.get("text"),
                "author_username": cast.get("author_username", ""),
                "author_fid": cast.get("author_fid"),
                "author_bio": "",
                # Default values for Neo4j fields
                "author_farcaster_cred_score": None,
                "wallet_eth_stables_value_usd": 0,
                "farcaster_usdc_rewards_earned": 0,
                "linked_accounts": [],
                "linked_wallets": [],
                "source": "mongo_raw"
            }
            
            # If we have FID enrichment data, update the structured cast
            if fid and fid in fid_enrichment_map:
                enr = fid_enrichment_map[fid]
                
                # Update with enrichment data
                enriched_cast["author_username"] = enr["authorUsername"] or cast.get("author_username", "")
                enriched_cast["author_bio"] = enr["authorBio"] or ""
                enriched_cast["author_farcaster_cred_score"] = enr["fcCredScore"]
                enriched_cast["wallet_eth_stables_value_usd"] = enr["walletEthStablesValueUsd"]
                enriched_cast["farcaster_usdc_rewards_earned"] = enr["farcaster_usdc_rewards_earned"]
                enriched_cast["linked_accounts"] = enr["linkedAccounts"]
                enriched_cast["linked_wallets"] = enr["linkedWallets"]
                enriched_cast["source"] = "mongo_enriched"
            
            enriched_mongo_casts.append(enriched_cast)
        
        # Combine all enriched casts
        combined_casts = enriched_mongo_casts
        
        # Sort final combined set by timestamp desc
        combined_casts.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        logger.info(f"Combined and sorted {len(combined_casts)} total casts")
        
        # Count by source for logging
        source_counts = {}
        for cast in combined_casts:
            source = cast.get("source", "unknown")
            source_counts[source] = source_counts.get(source, 0) + 1
        
        logger.info(f"Final cast sources: {source_counts}")
        
        # Log a sample of the final combined results (last 5)
        if combined_casts:
            sample_size = min(5, len(combined_casts))
            logger.info(f"Sample of last {sample_size} combined casts:")
            for i, cast in enumerate(combined_casts[-sample_size:]):
                logger.info(f"  Cast {i+1}: hash={cast.get('hash')}, author={cast.get('author_username')}, timestamp={cast.get('timestamp')}, source={cast.get('source', 'unknown')}")
                logger.info(f"    Text preview: {cast.get('text')[:50]}...")
        
        # ---------------------------------------------------------------------
        # 3) Save to JSON (optional, like your snippet), for debugging
        # ---------------------------------------------------------------------
        try:
            save_search_results_to_json(
                request.query, 
                combined_casts, 
                mongo_count=len(mongo_casts)
            )
        except Exception as e:
            logger.error(f"Error saving JSON: {str(e)}")
        
        # Calculate metrics for the response
        casts_count = len(combined_casts)
        
        # Calculate average fcCredScore for casts that have it
        cred_scores = [float(cast.get("author_farcaster_cred_score", 0)) for cast in combined_casts 
                      if cast.get("author_farcaster_cred_score") is not None]
        avg_cred_score = sum(cred_scores) / len(cred_scores) if cred_scores else 0
        
        # Get unique authors (FIDs) for diversity calculation
        unique_authors = set()
        for cast in combined_casts:
            if cast.get("author_fid"):
                unique_authors.add(cast.get("author_fid"))
        
        # Calculate diversity multiplier (similar to miniapp mentions)
        diversity_multiplier = min(1.0, len(unique_authors) / max(1, casts_count))
        
        # Calculate raw weighted score and apply diversity multiplier
        raw_weighted_score = casts_count * avg_cred_score
        weighted_score = raw_weighted_score * diversity_multiplier 
        
        # Create metrics dictionary
        metrics = {
            "casts": casts_count,
            "uniqueAuthors": len(unique_authors),
            "rawWeightedScore": raw_weighted_score,
            "diversityMultiplier": diversity_multiplier,
            "weighted_score": weighted_score,
        }
        
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        logger.info(f"Completed weighted casts search in {total_duration:.2f} seconds. Found {casts_count} casts from {len(unique_authors)} unique authors.")
        logger.info(f"Metrics: raw_score={raw_weighted_score:.2f}, diversity={diversity_multiplier:.2f}, weighted_score={weighted_score:.2f}")
        
        # Return all results with some basic metadata
        return {
            "casts": combined_casts,
            "total": len(combined_casts),
            "metrics": metrics
        }        
    except Exception as e:
        logger.error(f"Error retrieving weighted casts: {str(e)}")
        logger.exception("Detailed traceback:")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")