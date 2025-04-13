import os
import csv 
import httpx 
import json 
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, validator, root_validator
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
NEYNAR_API_KEY = os.getenv("NEYNAR_API_KEY")
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
    api_key: str = os.getenv("CLANK_PASS")
    token_address: Optional[str] = None

# Response models
# Update the TokenData model to match the return types in the query
class TokenData(BaseModel):
    address: str = Field(..., description="Token contract address")
    name: Optional[str] = Field(None, description="Token name")
    symbol: Optional[str] = Field(None, description="Token $symbol")
    believerScore: Optional[float] = Field(None, description="Normalized believer score (0-100)")
    rawBelieverScore: Optional[float] = Field(None, description="Raw believer score before adjustments")
    diversityAdjustedScore: Optional[float] = Field(None, description="Believer score adjusted for token concentration")
    marketAdjustedScore: Optional[float] = Field(None, description="Believer score adjusted for market cap ratio")
    holderToMarketCapRatio: Optional[float] = Field(None, description="Ratio of holders to market cap")
    marketCap: Optional[float] = Field(None, description="Token market capitalization")
    walletCount: Optional[float] = Field(None, description="Total unique wallet holders")
    warpcastWallets: Optional[float] = Field(None, description="Number of wallets connected to Warpcast accounts")
    warpcastPercentage: Optional[float] = Field(None, description="Percentage of wallets connected to Warpcast")
    avgSocialCredScore: Optional[float] = Field(None, description="Average holder social credibility")
    totalSupply: Optional[float] = Field(None, description="Total token supply")
    
    class Config:
        extra = "allow"  # Allow extra fields that may be returned by the API
        
    @root_validator(pre=True)
    def handle_null_values(cls, values):
        # Convert None or empty values to appropriate defaults
        for field in values:
            if values[field] is None and field in ['believerScore', 'rawBelieverScore', 
                                                  'diversityAdjustedScore', 'marketAdjustedScore',
                                                  'holderToMarketCapRatio', 'marketCap', 'walletCount',
                                                  'warpcastWallets', 'warpcastPercentage', 'totalSupply']:
                values[field] = 0.0
        return values

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
    
    class Config:
        extra = "allow"  # Allow extra fields

class MiniappMentionsData(BaseModel):
    mentions: List[Dict[str, Any]]
    
    class Config:
        extra = "allow"  # Allow extra fields

class MiniappMentionsResponse(BaseModel):
    data: Dict[str, Any]
    
    class Config:
        extra = "allow"  # Allow extra fields

# Define models for casts search
class CastRequest(BaseModel):
    query: str
    start_timestamp: Optional[int] = None
    end_timestamp: Optional[int] = None

class BelieversDataRequest(BaseModel):
    token_address: str = Field(..., description="Token contract address")
    
class PaginationInfo(BaseModel):
    count: int
    first_timestamp: Optional[str] = None
    last_timestamp: Optional[str] = None
    next_cursor: Optional[str] = None  # Added next_cursor to the model

class CastResponseData(BaseModel):
    casts: List[Dict]
    pagination: PaginationInfo    

class RecentCast(BaseModel):
    text: str = Field(..., description="Cast content")
    hash: str = Field(..., description="Unique cast identifier")
    timestamp: str = Field(..., description="Cast creation timestamp")

class TopBelieversData(BaseModel):
    fid: int = Field(..., description="User Farcaster ID.")
    username: str = Field(..., description="User Farcaster username.")
    bio: str = Field(..., description="User Farcaster Bio.")
    pfpUrl: str = Field(..., description="PFP URL for user.")
    fcred: float = Field(..., description="User Farcaster Cred Score (i.e. Social Cred Score).")
    walletBalance: float = Field(..., description="Estimated user wallet balance in ETH + USD stablecoins.")
    farcasterRewardsEarned: float = Field(..., description="Developer + Creator + Referral rewards paid to user by Farcaster.")


    
    # @validator('timestamp')
    # def validate_timestamp(cls, v):
    #     # Convert Neo4j DateTime objects to string if needed
    #     if hasattr(v, 'iso_format'):
    #         return v.iso_format()
    #     return v
    
    class Config:
        extra = "allow"  # Allow extra fields

class Promoter(BaseModel):
    username: str = Field(..., description="Social media username")
    fid: int = Field(..., description="Farcaster user identifier")
    fcCredScore: float = Field(..., description="Farcaster credibility score")
    recentCasts: List[Dict[str, Any]] = Field(..., description="Recent user posts")
    
    class Config:
        extra = "allow"  # Allow extra fields

class KeyPromotersData(BaseModel):
    promoters: List[Dict[str, Any]]
    
    class Config:
        extra = "allow"  # Allow extra fields

class KeyPromotersRequest(BaseModel):
    miniapp_name: str = Field(..., description="Name of the miniapp to retrieve key promoters for")

# Define models for weighted casts search response
class CastData(BaseModel):
    hash: str = Field(..., description="Unique cast identifier")
    timestamp: str = Field(..., description="Cast creation timestamp")
    text: str = Field(..., description="Cast content")
    author_username: str = Field(..., description="Author's username")
    author_fid: int = Field(..., description="Farcaster user ID")
    author_bio: Optional[str] = Field(None, description="Author's profile bio")
    author_farcaster_cred_score: Optional[float] = Field(None, description="Author credibility score")
    wallet_eth_stables_value_usd: Optional[float] = Field(
        None, 
        description="Total ETH/USDC balance across Mainnet, Base, Optimism, Arbitrum"
    )
    farcaster_usdc_rewards_earned: Optional[float] = Field(
        None, 
        description="Total USDC rewards from creator, developer, and referral programs"
    )
    linked_accounts: List[Dict[str, str]] = Field(default_factory=list, description="Linked social accounts")
    linked_wallets: List[Dict[str, str]] = Field(default_factory=list, description="Linked blockchain wallets")
    source: Optional[str] = Field(None, description="Data source")
    
    class Config:
        extra = "allow"  # Allow extra fields that may be returned by the API

class CastMetricsData(BaseModel):
    casts: int = Field(..., description="Total matching casts")
    uniqueAuthors: int = Field(..., description="Distinct cast authors")
    rawWeightedScore: float = Field(..., description="Unmodified credibility score")
    diversityMultiplier: float = Field(..., description="Author diversity coefficient - penalizes spammers")
    weighted_score: float = Field(..., description="Final credibility score")
    
    class Config:
        extra = "allow"  # Allow extra fields

class WeightedCastsResponseData(BaseModel):
    casts: List[Dict[str, Any]] = Field(..., description="Matching casts")
    total: int = Field(..., description="Total cast count")
    metrics: Dict[str, Any] = Field(..., description="Cast collection metrics")
    
    class Config:
        extra = "allow"  # Allow extra fields that may be returned by the API

# Define routes
@app.get("/")
async def root():
    return {"message": "Token API is running"}


@app.post(
    "/farstore-miniapp-mentions-counts", 
    summary="Get mentions data for miniapps",
    description="Retrieves mention counts and statistics for miniapps from Farstore. API key required for authentication.",
    tags=["Farstore"],
    responses={
        200: {"description": "Successfully retrieved miniapp mentions data"},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No miniapp mention data found"},
        500: {"description": "Internal Server Error"}
    }
)
async def farstore_miniapp_mentions(
    api_key: str = Query(..., description="API key for authentication", example="something.something")
) -> Dict[str, Any]:
    """
    Get mentions data for miniapps from farstore
    
    - Requires valid API key for authentication
    - Returns mentions counts, weighted scores, and unique casters for each miniapp
    """
    # Validate API key
    if api_key != FARSTORE_PASS:
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
    

@app.post(
    "/farstore-miniapp-key-promoters", 
    summary="Get key promoters for a miniapp",
    description="Retrieves key promoters and their recent casts for a specified miniapp. API key required for authentication.",
    tags=["Farstore"],
    responses={
        200: {"description": "Successfully retrieved key promoters data"},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No key promoters found"},
        500: {"description": "Internal Server Error"}
    }
)
async def retrieve_miniapp_key_promoters(
    request: KeyPromotersRequest, 
    api_key: str = Query(..., description="API key for authentication", example="password.lol")
) -> Dict[str, Any]:
    """
    Retrieve key promoters for provided miniapp
    
    - Requires valid API key for authentication
    - Returns top promoters with their FID, username, credibility score, and recent casts
    """
    # Validate API key
    if api_key != FARSTORE_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")
        
    try: 
        ### get casts
        query = """
        CALL db.index.fulltext.queryNodes("frames", $query) YIELD node, score
        WITH node as cast
        MATCH (cast)-[r:POSTED]-(wc:Warpcast:Account)
        WHERE NOT (wc)-[:CREATED]->(:Miniapp {frameUrl: $query})
        WITH wc, wc.fcCredScore as fcCredScore, wc.username as username, wc.fid as fid, cast
        ORDER BY fcCredScore DESC
        LIMIT 25
        MATCH (wc)-[:POSTED]->(cast)
        WITH wc, username, fid, fcCredScore, cast
        ORDER BY cast.timestamp DESC
        WITH wc, username, fid, fcCredScore, collect({text: cast.text, hash: cast.hash, timestamp: toString(cast.timestamp)})[0..3] as recentCasts
        WITH collect({
            username: username,
            fid: fid,
            fcCredScore: fcCredScore,
            recentCasts: recentCasts
        }) as promoters
        RETURN {promoters: promoters} as data
        """
        
        # Execute query with the miniapp_name parameter
        results = execute_cypher(query, {"query": request.miniapp_name})
        
        # Process results
        if not results or len(results) == 0:
            raise HTTPException(status_code=404, detail="No key promoters found")
        
        # Extract the data from the Neo4j result
        neo4j_data = results[0].get("data")
        promoters_data = neo4j_data.get("promoters", [])
        
        # Return the data directly as a dictionary
        return {"promoters": promoters_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    



@app.post(
    "/token-believer-score",
    summary="Get comprehensive token believer scores",
    description="Retrieves advanced believer scores with market cap adjustments and concentration metrics for tokens. Optionally filter by a specific token address.",
    tags=["Tokens"],
    responses={
        200: {"description": "Successfully retrieved token believer scores"},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No tokens found with the provided addresses"},
        500: {"description": "Internal Server Error"}
    }
)
async def retrieve_token_believer_scores(request: TokensRequest) -> Dict[str, Any]:
    """
    Retrieve comprehensive believer scores and supporting metadata for token addresses
    
    - Requires valid API key for authentication
    - Returns normalized believer scores (0-100) with detailed metrics
    - Includes market cap adjustments, token concentration, and social metrics
    - Provides raw and adjusted scores for transparency
    - Optionally filter by a specific token address
    """
    # Validate API key
    if request.api_key != os.getenv("CLANK_PASS"):
        raise HTTPException(status_code=401, detail="Invalid API key")
        
    try:
        # Advanced query that calculates normalized believer scores with market cap adjustments
        query = """
        // First, find necessary statistics across all tokens
        MATCH (token:Token)
        WITH token, token.marketCap AS marketCap
        // Calculate believer score without market cap factor first
        MATCH (wallet:Wallet)-[r:HOLDS]->(token:Token)
        WITH token, marketCap, count(wallet) AS num_wallets, collect(wallet) AS wallets
        // Count wallets connected to Warpcast
        MATCH (w:Wallet)-[r:HOLDS]->(token)
        MATCH (w)-[:ACCOUNT*1..5]-(wc:Warpcast:Account)
        WITH token, marketCap, num_wallets, count(DISTINCT w) AS warpcast_wallets
        // Calculate percentage of wallets connected to Warpcast
        WITH token, marketCap, num_wallets, warpcast_wallets,
             CASE 
                  WHEN num_wallets = 0 THEN 0.0
                  ELSE (toFloat(warpcast_wallets) * 100.0 / num_wallets)
             END AS warpcast_percentage

        // Recalculate full believer score
        MATCH (wallet:Wallet)-[r:HOLDS]->(token)
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, wallet, r.balance AS balance
        OPTIONAL MATCH path = (wallet)-[:ACCOUNT*1..5]-(wc:Warpcast)
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, wc, wallet, balance
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage,
             // Calculate total balance for this token
             sum(balance) AS total_balance,
             // For Gini calculation
             collect({wallet: wallet, balance: balance}) AS wallet_data,
             // Calculate social data
             avg(wc.fcCredScore) AS avgSocialCredScore

        // Calculate concentration score
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, total_balance, wallet_data, avgSocialCredScore
        UNWIND wallet_data AS wallet_item
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, total_balance, wallet_item.balance/total_balance AS balance_fraction, avgSocialCredScore
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, total_balance, 
             // Calculate concentration using HHI
             sum(balance_fraction * balance_fraction) AS concentration_index,
             avgSocialCredScore
             
        // Calculate believer score with concentration penalty
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, total_balance,
             // Concentration penalty - convert HHI to diversity score (1 - HHI)
             (1.0 - concentration_index) AS concentration_multiplier,
             avgSocialCredScore

        // Calculate standard believer score
        MATCH (wallet:Wallet)-[r:HOLDS]->(token)
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, wallet, concentration_multiplier, avgSocialCredScore, total_balance
        OPTIONAL MATCH path = (wallet)-[:ACCOUNT*1..5]-(wc:Warpcast)
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, wc, collect(DISTINCT wallet) AS wallet_group, concentration_multiplier, avgSocialCredScore, total_balance
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, wc, wallet_group, concentration_multiplier, avgSocialCredScore, total_balance,
             CASE WHEN wc IS NULL THEN size(wallet_group)
                  ELSE 1 + coalesce(wc.fcCredScore, 0)
             END AS group_weight

        // Calculate raw and diversity-adjusted believer scores
        WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, 
             sum(group_weight) AS raw_score,
             concentration_multiplier, 
             avgSocialCredScore,
             total_balance

        // Apply diversity adjustment
        WITH token,
             raw_score AS raw_believer_score,
             raw_score * concentration_multiplier AS diversity_adjusted_score,
             marketCap,
             num_wallets,
             warpcast_wallets,
             warpcast_percentage,
             avgSocialCredScore,
             total_balance

        // Calculate holder-to-market cap ratio
        WITH token, 
             raw_believer_score,
             diversity_adjusted_score,
             marketCap,
             num_wallets,
             warpcast_wallets,
             warpcast_percentage,
             avgSocialCredScore,
             total_balance,
             // Direct holder-to-market cap ratio
             CASE 
                  WHEN marketCap <= 0 THEN 999999999.0  // Prevent division by zero
                  ELSE toFloat(num_wallets) / marketCap 
             END AS holder_mcap_ratio

        // Apply LESS AGGRESSIVE tiered market cap penalty
        WITH token, 
             raw_believer_score,
             diversity_adjusted_score,
             // Apply more forgiving tiered penalties
             CASE 
                  WHEN holder_mcap_ratio > 2.0 THEN diversity_adjusted_score * 0.05  // Still severe but less extreme (95% reduction)
                  WHEN holder_mcap_ratio > 1.0 THEN diversity_adjusted_score * 0.25  // Less severe (75% reduction)
                  WHEN holder_mcap_ratio > 0.5 THEN diversity_adjusted_score * 0.50  // Moderate (50% reduction)
                  WHEN holder_mcap_ratio > 0.2 THEN diversity_adjusted_score * 0.75  // Mild (25% reduction)
                  ELSE diversity_adjusted_score  // No penalty
             END AS market_adjusted_score,
             marketCap,
             num_wallets,
             warpcast_wallets,
             warpcast_percentage,
             avgSocialCredScore,
             total_balance,
             holder_mcap_ratio

        // Get min/max values for normalization
        WITH 
             MIN(market_adjusted_score) AS min_score, 
             MAX(market_adjusted_score) AS max_score,
             COLLECT({
                  token: token, 
                  raw_score: raw_believer_score,
                  diversity_score: diversity_adjusted_score,
                  market_score: market_adjusted_score,
                  ratio: holder_mcap_ratio,
                  marketCap: marketCap,
                  num_wallets: num_wallets,
                  warpcast_wallets: warpcast_wallets,
                  warpcast_percentage: warpcast_percentage,
                  avg_social: avgSocialCredScore,
                  total_balance: total_balance
             }) AS all_token_data

        // Normalize scores to 0-100
        UNWIND all_token_data AS token_data
        WITH token_data.token AS token, 
             token_data.raw_score AS raw_believer_score,
             token_data.diversity_score AS diversity_adjusted_score,
             token_data.market_score AS market_adjusted_score,
             token_data.ratio AS holder_mcap_ratio,
             token_data.marketCap AS marketCap,
             token_data.num_wallets AS num_wallets,
             token_data.warpcast_wallets AS warpcast_wallets,
             token_data.warpcast_percentage AS warpcast_percentage,
             token_data.avg_social AS avgSocialCredScore,
             token_data.total_balance AS total_balance,
             min_score, max_score

        WITH token, raw_believer_score, diversity_adjusted_score, market_adjusted_score, holder_mcap_ratio, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, avgSocialCredScore, total_balance,
             // Normalize to 0-100 scale
             CASE 
                  WHEN max_score = min_score THEN 50.0 // Default to middle value if all scores are equal
                  ELSE 100.0 * (market_adjusted_score - min_score) / (max_score - min_score)
             END AS normalized_believer_score
        // Filter by token address if provided
        WHERE CASE 
            WHEN $token_address IS NOT NULL THEN toLower(token.address) = toLower($token_address)
            ELSE true
        END
        RETURN
            token.address AS address, 
            token.name AS name,
            token.symbol AS symbol,
            tofloat(normalized_believer_score) AS believerScore,
            tofloat(raw_believer_score) AS rawBelieverScore,
            tofloat(diversity_adjusted_score) AS diversityAdjustedScore,
            tofloat(market_adjusted_score) AS marketAdjustedScore,
            tofloat(holder_mcap_ratio) AS holderToMarketCapRatio,
            tofloat(marketCap) AS marketCap,
            tofloat(num_wallets) AS walletCount,
            tofloat(warpcast_wallets) AS warpcastWallets,
            tofloat(warpcast_percentage) AS warpcastPercentage,
            avgSocialCredScore,
            tofloat(total_balance) AS totalSupply
        ORDER BY believerScore DESC
        """

        params = {}
        if request.token_address:
            params["token_address"] = request.token_address.lower()
        else:
            params["token_address"] = None
        
        # Execute query
        logger.info(f"Querying for tokens with params: {params}")
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
        
        return response_data.model_dump()
    except Exception as e:
        logger.error(f"Error retrieving token believer scores: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post(
        "/token-top-believers",
        summary="Top believers for token",
        description="Return top 25 believers for Product Clank-listed token",
        tags=["Tokens"],
        responses={
        200: {"description": "Successfully retrieved believers"},
        404: {"description": "No believers found for the token"},
        500: {"description": "Internal Server Error"}
    },
)
async def get_token_top_believers(request: BelieversDataRequest) -> Dict[str, Any]:
    """
    Get top 25 believers for a specific token
    
    - Returns believers with their wallet and Warpcast account information
    """
    try:
        # Lowercase the token address
        token_address = request.token_address.lower()
        
        # Query to find top believers
        query = """
      MATCH (believerWallet:Wallet)-[r:HOLDS]->(token:Token {address:$token_address})
        MATCH (believerWallet)-[:ACCOUNT*..4]-(wc:Warpcast:Account)  
        WHERE wc.fcCredScore is not null       
        ORDER BY wc.fcCredScore DESC LIMIT 25
        RETURN {
            top_believers: COLLECT(DISTINCT({
                fid: tointeger(wc.fid),
                username: wc.username,
                bio: wc.bio,
                pfpUrl: wc.pfpUrl,
                fcred: wc.fcCredScore
            }))
        } as data"""
        
        params = {"token_address": token_address}
        # Execute query
        results = execute_cypher(query, params)
        
        # Process results
        if not results or len(results) == 0:
            raise HTTPException(status_code=404, detail="No believers found for the token")
        
        # Extract the data from the Neo4j result
        neo4j_data = results[0].get("data")
        believers_data = neo4j_data.get("top_believers", [])
        
        # Return the data in the expected format
        return {"believers": believers_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    


def clean_query_for_lucene(user_query):
    if not user_query:
        return ""    
    special_chars = ['/', '\\', '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '~', '*', '?', ':', '"']
    cleaned_query = user_query
    for char in special_chars:
        cleaned_query = cleaned_query.replace(char, ' ')

    cleaned_query = ' '.join(cleaned_query.split())
    
    return cleaned_query

@app.post(
    "/casts-search-weighted",
    summary="Search for casts with weighted scoring",
    description="Search for casts matching a query with weighted scoring based on author credibility. API key required for authentication.",
    tags=["Search"],
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
    Get matching casts and related metadata using a hybrid Neynar API + Neo4j approach.
    Returns all matching results without pagination.
    
    - Requires valid API key for authentication
    """
    # Validate API key
    if api_key != os.getenv('FART_PASS'):
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
        # 0) Clean the user's query for Neo4j fulltext
        # ---------------------------------------------------------------------
        clean_query = clean_query_for_lucene(request.query)
        logger.info(f"User's raw search: '{request.query}', cleaned for Neo4j: '{clean_query}'")
        
        # ---------------------------------------------------------------------
        # 1) Fetch from Neo4j (one-time)
        #    We'll just do a single pass for all relevant casts from Graph
        # ---------------------------------------------------------------------
        neo4j_query = """
        CALL db.index.fulltext.queryNodes("casts", $query) YIELD node, score
        WITH node, score 
        WHERE score > 4
        AND node.timestamp IS NOT NULL
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
        RETURN {
            hash: hash,
            timestamp: toString(timestamp),
            text: castText,
            author_username: authorUsername,
            wallet_eth_stables_value_usd: walletEthStablesValueUsd,
            author_fid: authorFid,
            author_bio: authorBio,
            author_farcaster_cred_score: fcs,
            farcaster_usdc_rewards_earned: farcaster_usdc_rewards_earned,
            linked_accounts: [acc IN linkedAccounts WHERE acc.platform <> "Wallet"],
            linked_wallets: linkedWallets
        } as cast
        """
        
        neo4j_params = {"query": clean_query}
        logger.info(f"Executing Neo4j query with params: {neo4j_params}")
        neo4j_start_time = datetime.now()
        
        neo4j_results = execute_cypher(neo4j_query, neo4j_params)
        neo4j_end_time = datetime.now()
        neo4j_duration = (neo4j_end_time - neo4j_start_time).total_seconds()
        logger.info(f"Neo4j query completed in {neo4j_duration:.2f} seconds, returned {len(neo4j_results)} results")
        
        neo4j_casts = [record.get("cast") for record in neo4j_results]
        
        # Log a sample of the Neo4j results (last 5)
        if neo4j_casts:
            sample_size = min(5, len(neo4j_casts))
            logger.info(f"Sample of last {sample_size} Neo4j casts:")
            for i, cast in enumerate(neo4j_casts[-sample_size:]):
                logger.info(f"  Cast {i+1}: hash={cast.get('hash')}, author={cast.get('author_username')}, timestamp={cast.get('timestamp')}")
                logger.info(f"    Text preview: {cast.get('text')[:50]}...")
        
        # ---------------------------------------------------------------------
        # 2) Repeated calls to Neynar with cursor, stopping at 2025-03-31
        # ---------------------------------------------------------------------
        neynar_casts = []
        neynar_api_key = NEYNAR_API_KEY
        
        if not neynar_api_key:
            logger.warning("Neynar API key not found; skipping Neynar calls.")
        else:
            # We'll keep calling until we see a cast at or before `2025-03-31`,
            # or until `nextCursor` is not returned.
            
            neynar_url = "https://api.neynar.com/v2/farcaster/cast/search"
            headers = {
                "accept": "application/json",
                "x-api-key": neynar_api_key
            }
            
            neynar_query = f"{request.query} after:2025-03-31"
            
            # We'll start with no `cursor`
            current_cursor = None
            keep_going = True
            neynar_call_count = 0
            neynar_start_time = datetime.now()
            
            while keep_going:
                params = {
                    "q": neynar_query,
                    "limit": 100,                
                    "sort_type": "desc_chron",
                }
                if current_cursor:
                    params["cursor"] = current_cursor
                
                logger.info(f"Calling Neynar API (call #{neynar_call_count+1}) with params: {params}")
                call_start_time = datetime.now()
                
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.get(neynar_url, headers=headers, params=params)
                    
                    call_end_time = datetime.now()
                    call_duration = (call_end_time - call_start_time).total_seconds()
                    neynar_call_count += 1
                    
                    if response.status_code != 200:
                        logger.error(f"Neynar API error: {response.status_code} - {response.text}")
                        break  # exit loop, can't do anything more
                        
                    neynar_data = response.json()
                    
                    # Extract casts and nextCursor
                    neynar_raw_casts = neynar_data.get("result", {}).get("casts", [])
                    next_cursor = neynar_data.get("result", {}).get("nextCursor", None)
                    
                    logger.info(f"Received {len(neynar_raw_casts)} casts from Neynar in {call_duration:.2f} seconds. nextCursor: {next_cursor}")
                    
                    # Convert raw Neynar data to a simpler structure
                    for c in neynar_raw_casts:
                        neynar_casts.append({
                            "hash": c.get("hash"),
                            "timestamp": c.get("timestamp"),
                            "text": c.get("text"),
                            "author_fid": c.get("author", {}).get("fid"),
                            "author_username": c.get("author", {}).get("username"),
                            "author_bio": c.get("author", {}).get("bio", "")
                        })
                    
                    # Check if there's any cast with timestamp <= "2025-03-31"
                    cutoff_date_str = "2025-03-31T00:00:00Z"
                    for c in neynar_raw_casts:
                        cast_time = c.get("timestamp", "")
                        if cast_time <= cutoff_date_str:
                            logger.info(f"Found a cast at or before 2025-03-31 (timestamp: {cast_time}); stopping further Neynar paging.")
                            keep_going = False
                            break
                    
                    # If we didn't break, and there's no nextCursor, we also stop
                    if keep_going and not next_cursor:
                        logger.info("Neynar has no further results. Stopping.")
                        keep_going = False
                    else:
                        # Move to next page
                        current_cursor = next_cursor
                    
                except Exception as api_error:
                    logger.error(f"Error calling Neynar API: {str(api_error)}")
                    keep_going = False
            
            neynar_end_time = datetime.now()
            neynar_duration = (neynar_end_time - neynar_start_time).total_seconds()
            logger.info(f"Completed {neynar_call_count} Neynar API calls in {neynar_duration:.2f} seconds, retrieved {len(neynar_casts)} total casts")
            
            # Log a sample of the Neynar results (last 5)
            if neynar_casts:
                sample_size = min(5, len(neynar_casts))
                logger.info(f"Sample of last {sample_size} Neynar casts:")
                for i, cast in enumerate(neynar_casts[-sample_size:]):
                    logger.info(f"  Cast {i+1}: hash={cast.get('hash')}, author={cast.get('author_username')}, timestamp={cast.get('timestamp')}")
                    logger.info(f"    Text preview: {cast.get('text')[:50]}...")
        
        # ---------------------------------------------------------------------
        # 3) Combine + De-duplicate (by cast hash)
        # ---------------------------------------------------------------------
        # We need a more robust enrichment approach
        
        # First, try to directly enrich Neynar casts by looking up their hashes in Neo4j
        neynar_hashes = [cast.get("hash") for cast in neynar_casts if cast.get("hash")]
        logger.info(f"Looking up {len(neynar_hashes)} Neynar cast hashes in Neo4j for direct enrichment")
        
        # Direct cast enrichment query - find these exact casts in Neo4j
        direct_enrichment_start_time = datetime.now()
        cast_enrichment_query = """
        MATCH (cast:Cast)
        WHERE cast.hash IN $hashes
        MATCH (cast)-[:POSTED]-(wc:Warpcast:Account)
        OPTIONAL MATCH (wc)-[:ACCOUNT]-(wallet:Wallet)
        OPTIONAL MATCH (wc)-[:ACCOUNT]-(account:Account)
        OPTIONAL MATCH ()-[rewards:REWARDS]->(:Wallet)-[:ACCOUNT]-(wc:Warpcast:Account)
        WITH 
            cast.hash as hash,
            cast.timestamp as timestamp, 
            cast.text as castText,
            wc.username as authorUsername,
            tofloat(sum(coalesce(tofloat(wallet.balance), 0))) as walletEthStablesValueUsd,
            wc.fid as authorFid,
            wc.bio as authorBio,
            wc.fcCredScore as fcs,
            tofloat(sum(coalesce(tofloat(rewards.value), 0))) as farcaster_usdc_rewards_earned,
            collect(distinct({platform: account.platform, username: account.username})) as linkedAccounts,
            collect(distinct({address: wallet.address, network: wallet.network})) as linkedWallets
        RETURN {
            hash: hash,
            timestamp: toString(timestamp),
            text: castText,
            author_username: authorUsername,
            wallet_eth_stables_value_usd: walletEthStablesValueUsd,
            author_fid: authorFid,
            author_bio: authorBio, 
            author_farcaster_cred_score: fcs,
            farcaster_usdc_rewards_earned: farcaster_usdc_rewards_earned,
            linked_accounts: [acc IN linkedAccounts WHERE acc.platform <> "Wallet"],
            linked_wallets: linkedWallets
        } as cast
        """
        
        # Execute the direct cast enrichment
        direct_enrichment_results = []
        if neynar_hashes:
            direct_enrichment_results = execute_cypher(cast_enrichment_query, {"hashes": neynar_hashes})
        
        direct_enriched_casts = [record.get("cast") for record in direct_enrichment_results]
        directly_enriched_hashes = {cast.get("hash") for cast in direct_enriched_casts}
        
        direct_enrichment_end_time = datetime.now()
        direct_enrichment_duration = (direct_enrichment_end_time - direct_enrichment_start_time).total_seconds()
        logger.info(f"Directly enriched {len(directly_enriched_hashes)} casts from Neo4j by hash in {direct_enrichment_duration:.2f} seconds")
        
        # Find Neynar casts that weren't directly enriched by hash
        remaining_neynar_casts = [cast for cast in neynar_casts if cast.get("hash") not in directly_enriched_hashes]
        logger.info(f"Still need to enrich {len(remaining_neynar_casts)} Neynar casts by FID")
        
        # For the remaining casts, use FID-based enrichment as before
        remaining_fids = {
            str(c.get("author_fid")) for c in remaining_neynar_casts if c.get("author_fid")
        }
        
        # FID enrichment map for the remaining casts
        fid_enrichment_map = {}
        fid_enriched_casts = []
        
        # Always attempt to enrich remaining Neynar casts if there are any
        if remaining_fids:
            logger.info(f"Found {len(remaining_fids)} unique FIDs in remaining Neynar casts to enrich from Neo4j")
            fid_enrichment_start_time = datetime.now()
            
            # Enrich them from Neo4j
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
            
            enrichment_results = execute_cypher(enrichment_query, {"fids": list(remaining_fids)})
            
            fid_enrichment_end_time = datetime.now()
            fid_enrichment_duration = (fid_enrichment_end_time - fid_enrichment_start_time).total_seconds()
            logger.info(f"FID enrichment query completed in {fid_enrichment_duration:.2f} seconds, returned data for {len(enrichment_results)} FIDs")
            
            # Build a map from fid -> dict of enrichment
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
            
            # Apply FID-based enrichment to the remaining casts
            enriched_count = 0
            for cast in remaining_neynar_casts:
                fid = cast.get("author_fid")
                
                # Create a structured cast with all required fields
                enriched_cast = {
                    "hash": cast.get("hash"),
                    "timestamp": cast.get("timestamp"),
                    "text": cast.get("text"),
                    "author_username": cast.get("author_username", ""),
                    "author_fid": cast.get("author_fid"),
                    "author_bio": cast.get("author_bio", ""),
                    # Default values for Neo4j fields
                    "author_farcaster_cred_score": None,
                    "wallet_eth_stables_value_usd": 0,
                    "farcaster_usdc_rewards_earned": 0,
                    "linked_accounts": [],
                    "linked_wallets": []
                }
                
                # If we have FID enrichment data, update the structured cast
                if fid and fid in fid_enrichment_map:
                    enriched_count += 1
                    enr = fid_enrichment_map[fid]
                    
                    # Update with enrichment data
                    enriched_cast["author_username"] = enr["authorUsername"] or cast.get("author_username", "")
                    enriched_cast["author_bio"] = enr["authorBio"] or cast.get("author_bio", "")
                    enriched_cast["author_farcaster_cred_score"] = enr["fcCredScore"]
                    enriched_cast["wallet_eth_stables_value_usd"] = enr["walletEthStablesValueUsd"]
                    enriched_cast["farcaster_usdc_rewards_earned"] = enr["farcaster_usdc_rewards_earned"]
                    enriched_cast["linked_accounts"] = enr["linkedAccounts"]
                    enriched_cast["linked_wallets"] = enr["linkedWallets"]
                
                # Add a source marker
                enriched_cast["source"] = "neynar_fid_enriched" if fid and fid in fid_enrichment_map else "neynar_raw"
                
                # Add to the list of FID-enriched casts
                fid_enriched_casts.append(enriched_cast)
            
            logger.info(f"Successfully enriched {enriched_count} of {len(remaining_neynar_casts)} remaining Neynar casts with Neo4j FID data")
        
        # Start with Neo4j casts and mark them
        combined_casts = list(neo4j_casts)
        for cast in combined_casts:
            cast["source"] = "neo4j_search"
        
        # Create a set of hashes we already have
        existing_hashes = {c.get("hash") for c in combined_casts}
        
        # Add the directly enriched casts (if not already in the results)
        for cast in direct_enriched_casts:
            if cast.get("hash") not in existing_hashes:
                cast["source"] = "neo4j_direct"
                combined_casts.append(cast)
                existing_hashes.add(cast.get("hash"))
        
        # Finally, add the FID-enriched/raw Neynar casts (if not already in the results)
        for cast in fid_enriched_casts:
            if cast.get("hash") not in existing_hashes:
                # Source is already set in the fid_enriched_casts creation
                combined_casts.append(cast)
                existing_hashes.add(cast.get("hash"))
        
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
        # 4) Save to JSON (optional, like your snippet), for debugging
        # ---------------------------------------------------------------------
        try:
            os.makedirs("data/query_results", exist_ok=True)
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            clean_filename = clean_query_for_lucene(request.query) or "empty_query"
            json_filename = f"data/query_results/{clean_filename}_{timestamp_str}.json"
            
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump({
                    "query": request.query,
                    "timestamp": datetime.now().isoformat(),
                    "neo4j_count": len(neo4j_casts),
                    "neynar_count": len(neynar_casts),
                    "unique_neynar_count": len(remaining_neynar_casts),
                    "total_count": len(combined_casts),
                    "casts": combined_casts
                }, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved search results to {json_filename}")
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
        
@app.on_event("shutdown")
async def shutdown_event():
    """Close Neo4j driver connection when app shuts down"""
    logger.info("Shutting down application, closing Neo4j connection")
    neo4j_driver.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)