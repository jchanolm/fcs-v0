import os
import csv 
import httpx 
import json 
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional, Union
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(override=True)

FARSTORE_PASS = os.getenv('FARSTORE_PASS')
# Neo4j Configuration
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEYNAR_API_KEY = os.getenv("")
# Database will be None, which will use the default database
NEO4J_DATABASE = None

# Initialize Neo4j driver
try:
    logger.info(f"Connecting to Neo4j with URI: {NEO4J_URI}")
    logger.info(f"Username: {NEO4J_USERNAME}")
    logger.info(f"Password: {'*' * len(NEO4J_PASSWORD) if NEO4J_PASSWORD else 'None'}")
    
    neo4j_driver = GraphDatabase.driver(
        NEO4J_URI, 
        auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )
    
    # Test the connection right away
    with neo4j_driver.session() as session:
        result = session.run("RETURN 1 as test")
        for record in result:
            logger.info(f"Neo4j connection test successful: {record['test']}")
            
except Exception as e:
    logger.error(f"Neo4j connection error: {str(e)}")
    # Don't raise here, just log the error

# Initialize FastAPI
app = FastAPI(title="Token API", description="API for querying token data from Neo4j")

def execute_cypher(query, params=None):
    """Execute a Cypher query in Neo4j"""
    # Using None for database parameter will use the default database
    with neo4j_driver.session(database=NEO4J_DATABASE) as session:
        result = session.run(query, params)
        return list(result)

# Request models
class TokensRequest(BaseModel):
    token_addresses: List[str] = Field(..., description="List of token addresses to query")

# Response models
class TokenData(BaseModel):
    address: str = Field(..., description="Token contract address")
    name: Optional[str] = Field(None, description="Token name")
    symbol: Optional[str] = Field(None, description="Token symbol")
    believerScore: float = Field(..., description="Calculated believer score for the token")
    holderCount: float = Field(..., description="Number of token holders")
    avgSocialCredScore: Optional[float] = Field(None, description="Average social credibility score")

class TokenResponseData(BaseModel):
    fcs_data: List[TokenData] = Field(..., description="List of token data with believer scores")

class MiniappMentionData(BaseModel):
    name: str = Field(..., description="Miniapp name")
    frameUrl: str = Field(..., description="Frame URL")
    mentions: int = Field(..., description="Number of mentions")
    fcsWeightedMentions: float = Field(..., description="FCS weighted mentions")

# Define response models for the farstore-miniapp-mentions-counts endpoint
class MiniappMention(BaseModel):
    name: str
    frameUrl: str
    mentionsAllTime: Optional[float] = 0.0
    uniqueCasters: Optional[int] = 0
    rawWeightedCasts: Optional[float] = 0.0
    weightedCasts: Optional[float] = 0.0
    avgFcsCredScore: Optional[float] = 0.0

class MiniappMentionsData(BaseModel):
    mentions: List[MiniappMention]

class MiniappMentionsResponse(BaseModel):
    data: MiniappMentionsData

# Define models for casts search
class CastRequest(BaseModel):
    query: str
    start_timestamp: Optional[int] = None
    end_timestamp: Optional[int] = None

class PaginationInfo(BaseModel):
    count: int
    first_timestamp: Optional[str] = None
    last_timestamp: Optional[str] = None

class CastResponseData(BaseModel):
    casts: List[Dict]
    pagination: PaginationInfo    


# Define routes
@app.get("/")
async def root():
    return {"message": "Token API is running"}

@app.post("/farstore-miniapp-mentions-counts", response_model=MiniappMentionsResponse)
async def farstore_miniapp_mentions(api_key: str = Query(..., description="API key for authentication")):
    """Get mentions data for miniapps from farstore"""
    # Validate API key
    if api_key != "password.lol":
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    try:
        # Neo4j query to fetch miniapp mentions
        query = """
        MATCH
            (m:Miniapp:Farstore)
        WITH 
            COLLECT(DISTINCT {
            name: m.name,
            frameUrl: m.frameUrl,
            mentionsAllTime: tofloat(m.mentionsAllTime),
            uniqueCasters: tointeger(m.uniqueCasters),
            rawWeightedCasts: tofloat(m.rawWeightedCasts),
            weightedCasts: tofloat(m.weightedCastsDiversityMultiplier),
            avgFcsCredScore: tofloat(m.avgCredScore)
            }) as mentions_counts
        RETURN
            {
                mentions: mentions_counts
            } as data
        """
        # Execute query
        results = execute_cypher(query)
        
        # Process results
        if not results or len(results) == 0:
            raise HTTPException(status_code=404, detail="No miniapp mention data found")
        
        # Extract the data from the Neo4j result and convert it to the expected format
        neo4j_data = results[0].get("data")
        mentions_data = neo4j_data.get("mentions", [])
        
        # Create a valid response object
        response_data = {
            "data": {
                "mentions": mentions_data
            }
        }
        
        return response_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/token-believer-score", response_model=TokenResponseData)
async def retrieve_token_believer_scores(request: TokensRequest) -> TokenResponseData:
    """Retrieve believer scores and supporting metadata for up to 25 Base token addresses"""
    try:
        # Query that accepts a list of token addresses
        query = """
      // Filter tokens by the provided addresses
      MATCH (token:Token)
      WHERE token.address IN $token_addresses
      
      // For each token, find all wallet holders
      MATCH (wallet:Wallet)-[:HOLDS]->(token)
      
      // Find all Warpcast accounts connected to these wallets (directly or through a path)
      WITH token, wallet
      OPTIONAL MATCH path = (wallet)-[:ACCOUNT*1..5]-(wc:Warpcast)
      
      // Group wallets by token and connected Warpcast account (if any)
      WITH token, wc, collect(DISTINCT wallet) AS wallet_group
      
      // Calculate weight for each group
      WITH token, wc, 
           CASE WHEN wc IS NULL THEN size(wallet_group) // Each unconnected wallet counts as 1
                ELSE 1 + coalesce(wc.fcCredScore, 0) // Connected wallets count as 1 + fcCredScore for the group
           END AS group_weight
      
      // Sum all weights for each token
      WITH token, sum(group_weight) AS weighted_holders, avg(wc.fcCredScore) as avgSocialCredScore
      
      // Return data for each token
      RETURN DISTINCT
       token.address as address, 
       token.name as name,
       token.symbol as symbol,
       tofloat(weighted_holders) as believerScore,
       tofloat(token.holderCount) as holderCount,
       avgSocialCredScore
    """

        requested_token_addresses = [x.lower() for x in request.token_addresses]
        params = {"token_addresses": requested_token_addresses}
        
        # Execute query
        logger.info(f"Querying for tokens: {requested_token_addresses}")
        results = execute_cypher(query, params)
        
        # Process results
        if not results:
            raise HTTPException(status_code=404, detail="No tokens found with the provided addresses")
        
        # Convert Neo4j records to Pydantic models
        token_list = []
        for record in results:
            # Convert Neo4j record to dict and create TokenData object
            record_dict = dict(record)
            token_data = TokenData(**record_dict)
            token_list.append(token_data)
        
        response_data = TokenResponseData(fcs_data=token_list)
        
        return response_data
    except Exception as e:
        logger.error(f"Error retrieving token believer scores: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Keep the original single token endpoint for backward compatibility
@app.post("/token", response_model=TokenResponseData)
async def get_token_data(request: TokensRequest) -> TokenResponseData:
    """Get data for a single token (redirects to /tokens endpoint)"""
    return await retrieve_token_believer_scores(request)


def clean_query_for_lucene(user_query):
    if not user_query:
        return ""    
    special_chars = ['/', '\\', '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '~', '*', '?', ':', '"']
    cleaned_query = user_query
    for char in special_chars:
        cleaned_query = cleaned_query.replace(char, ' ')

    cleaned_query = ' '.join(cleaned_query.split())
    
    return cleaned_query

import os
import json
import httpx
from datetime import datetime

@app.post("/casts-search-weighted", response_model=CastResponseData)
async def fetch_weighted_casts(request: CastRequest) -> CastResponseData:
    """Get matching casts and related metadata using hybrid Neynar API + Neo4j approach"""

    try:
        # Clean the query for Lucene
        clean_query = clean_query_for_lucene(request.query)
        logger.info(f"Searching for: {request.query}, cleaned: {clean_query}")
        
        # Step 1: Get results from Neo4j
        neo4j_query = """
        CALL db.index.fulltext.queryNodes("casts", $query) YIELD node
        WITH node
        WHERE node.timestamp IS NOT NULL
        MATCH (node)-[:POSTED]-(wc:Warpcast:Account)
        OPTIONAL MATCH (wc)-[:ACCOUNT]-(wallet:Wallet)
        OPTIONAL MATCH (wc)-[:ACCOUNT]-(account:Account)
        OPTIONAL MATCH ()-[rewards:REWARDS]->(:Wallet)-[:ACCOUNT]-(wc:Warpcast:Account)
        WITH 
            node.hash as hash,
            node.timestamp as timestamp, 
            node.text as castText,
            wc.username as authorUsername,
            tofloat(sum(coalesce(tofloat(wallet.balance), 0))) as walletEthStablesValueUsd,
            wc.fid as authorFid,
            wc.bio as authorBio,
            wc.fcCredScore as fcs,
            tofloat(sum(coalesce(tofloat(rewards.value), 0))) as farcaster_usdc_rewards_earned,
            collect(distinct({platform: account.platform, username: account.username})) as linkedAccounts,
            collect(distinct({address: wallet.address, network: wallet.network})) as linkedWallets
        ORDER BY timestamp DESC
        LIMIT 1000
        RETURN {
            hash: hash,
            timestamp: timestamp, 
            text: castText,
            author_username: authorUsername,
            wallet_eth_stables_value_usd: walletEthStablesValueUsd,
            author_fid: authorFid,
            author_bio: authorBio,
            author_farcaster_cred_score: fcs,
            farcaster_usdc_rewards_earned: farcaster_usdc_rewards_earned,
            linked_accounts: [acc IN linkedAccounts WHERE acc.platform <> "Wallet"],
            linked_wallets: linkedWallets,
            source: "neo4j"
        } as cast
        """
        
        # Execute Neo4j query
        neo4j_params = {"query": clean_query}
        logger.info(f"Executing Neo4j query with params: {neo4j_params}")
        
        neo4j_results = execute_cypher(neo4j_query, neo4j_params)
        logger.info(f"Neo4j query returned {len(neo4j_results)} results")
        
        # Extract casts from Neo4j results
        neo4j_casts = [record.get("cast") for record in neo4j_results]
        
        # Step 2: Get results from Neynar API 
        neynar_casts = []
        neynar_api_key = os.getenv("NEYNAR_API_KEY")
        if not neynar_api_key:
            logger.error("Neynar API key not found in environment variables")
        else:
            # Prepare Neynar API request
            neynar_url = "https://api.neynar.com/v2/farcaster/cast/search"
            headers = {
                "accept": "application/json",
                "x-api-key": neynar_api_key
            }
            
            # Add simple "after" filter to focus on newer casts
            neynar_query = f"{request.query} after:2025-03-31"
            
            params = {
                "q": neynar_query,
                "limit": 100,
                "sort_type": "desc_chron"
            }
            
            logger.info(f"Calling Neynar API with params: {params}")
            
            try:
                # Make API request
                async with httpx.AsyncClient() as client:
                    response = await client.get(neynar_url, headers=headers, params=params)
                
                if response.status_code != 200:
                    logger.error(f"Neynar API error: {response.status_code} - {response.text}")
                else:
                    # Parse response
                    neynar_data = response.json()
                    neynar_raw_casts = neynar_data.get("result", {}).get("casts", [])
                    
                    logger.info(f"Received {len(neynar_raw_casts)} casts from Neynar API")
                    
                    # Pre-process Neynar casts
                    for cast in neynar_raw_casts:
                        neynar_casts.append({
                            "hash": cast.get("hash"),
                            "timestamp": cast.get("timestamp"),
                            "text": cast.get("text"),
                            "author_fid": cast.get("author", {}).get("fid"),
                            "author_username": cast.get("author", {}).get("username"),
                            "author_bio": cast.get("author", {}).get("bio", ""),
                            "source": "neynar"
                        })
            except Exception as api_error:
                logger.error(f"Error calling Neynar API: {str(api_error)}")
        
        # Step 3: Combine and deduplicate results
        combined_casts = neo4j_casts.copy()
        neo4j_hashes = {cast.get("hash") for cast in neo4j_casts}
        
        # Add Neynar casts that aren't in Neo4j results
        neynar_only_casts = [cast for cast in neynar_casts if cast.get("hash") not in neo4j_hashes]
        logger.info(f"Found {len(neynar_only_casts)} casts from Neynar that aren't in Neo4j")
        
        # Step 4: Enrich Neynar-only casts if they have FIDs
        if neynar_only_casts:
            neynar_only_fids = [cast.get("author_fid") for cast in neynar_only_casts 
                               if cast.get("author_fid")]
            
            if neynar_only_fids:
                # Query Neo4j for enrichment data for these FIDs
                enrichment_query = """
                MATCH (wc:Warpcast:Account)
                WHERE wc.fid IN $fids
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
                RETURN fid, authorUsername, authorBio, fcCredScore, walletEthStablesValueUsd, 
                       farcaster_usdc_rewards_earned, 
                       [acc IN linkedAccounts WHERE acc.platform <> "Wallet"] as linkedAccounts, 
                       linkedWallets
                """
                
                enrichment_results = execute_cypher(enrichment_query, {"fids": neynar_only_fids})
                
                # Build FID to enrichment map
                enrichment_map = {}
                for record in enrichment_results:
                    fid = record.get("fid")
                    if fid:
                        enrichment_map[fid] = {
                            "authorUsername": record.get("authorUsername"),
                            "authorBio": record.get("authorBio"),
                            "fcCredScore": record.get("fcCredScore"),
                            "walletEthStablesValueUsd": record.get("walletEthStablesValueUsd"),
                            "farcaster_usdc_rewards_earned": record.get("farcaster_usdc_rewards_earned"),
                            "linkedAccounts": record.get("linkedAccounts", []),
                            "linkedWallets": record.get("linkedWallets", [])
                        }
                
                # Enrich and add Neynar-only casts
                for cast in neynar_only_casts:
                    author_fid = cast.get("author_fid")
                    enrichment = enrichment_map.get(author_fid, {})
                    
                    # Apply enrichment where available
                    if enrichment:
                        cast["author_username"] = enrichment.get("authorUsername") or cast.get("author_username")
                        cast["author_bio"] = enrichment.get("authorBio") or cast.get("author_bio")
                        cast["author_farcaster_cred_score"] = enrichment.get("fcCredScore")
                        cast["wallet_eth_stables_value_usd"] = enrichment.get("walletEthStablesValueUsd")
                        cast["farcaster_usdc_rewards_earned"] = enrichment.get("farcaster_usdc_rewards_earned")
                        cast["linked_accounts"] = enrichment.get("linkedAccounts")
                        cast["linked_wallets"] = enrichment.get("linkedWallets")
                    
                    combined_casts.append(cast)
        
        # Step 5: Save results to JSON for later ingestion
        os.makedirs("data/query_results", exist_ok=True)
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        clean_filename = clean_query_for_lucene(request.query)
        json_filename = f"data/query_results/{clean_filename}_{timestamp_str}.json"
        
        try:
            with open(json_filename, 'w', encoding='utf-8') as jsonfile:
                json.dump({
                    "query": request.query,
                    "timestamp": datetime.now().isoformat(),
                    "neo4j_count": len(neo4j_casts),
                    "neynar_count": len(neynar_casts),
                    "unique_neynar_count": len(neynar_only_casts),
                    "total_count": len(combined_casts),
                    "casts": combined_casts
                }, jsonfile, ensure_ascii=False, indent=2)
            logger.info(f"Saved search results to {json_filename}")
        except Exception as e:
            logger.error(f"Error saving JSON: {str(e)}")
        
        # Step 6: Sort combined results by timestamp
        combined_casts.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        # Get first and last timestamps for cursor functionality
        timestamps = [cast.get("timestamp") for cast in combined_casts if cast.get("timestamp")]
        first_timestamp = min(timestamps) if timestamps else None
        last_timestamp = max(timestamps) if timestamps else None
        
        # Prepare response
        response_data = {
            "casts": combined_casts,
            "pagination": {
                "count": len(combined_casts),
                "first_timestamp": first_timestamp,
                "last_timestamp": last_timestamp
            }
        }
        return response_data
        
    except Exception as e:
        logger.error(f"Error retrieving weighted casts: {str(e)}")
        logger.exception("Detailed traceback:")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")    

@app.on_event("shutdown")
async def shutdown_event():
    """Close Neo4j driver connection when app shuts down"""
    logger.info("Shutting down application, closing Neo4j connection")
    neo4j_driver.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)