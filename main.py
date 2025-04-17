import os
import csv 
import httpx 
import json 
from pymongo import MongoClient
from typing import List, Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorClient
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

# Initialize MongoDB here
mongo_client = None
async_mongo_client = None
db = None
async_db = None

def init_mongodb():
    """Initialize MongoDB connections."""
    global mongo_client, async_mongo_client, db, async_db
    
    try:
        MONGO_DB_URL = os.getenv("MONGO_DB_URL")
        MONGO_DB_NAME = "quotient"
        mongo_client = MongoClient(MONGO_DB_URL)
        
        # Async client for API operations
        async_mongo_client = AsyncIOMotorClient(MONGO_DB_URL)
        
        # Database references
        db = mongo_client[MONGO_DB_NAME]
        async_db = async_mongo_client[MONGO_DB_NAME]
        
        # Test the connection
        db_info = mongo_client.server_info()
        logger.info(f"Connected to MongoDB: {db_info.get('version')}")
        
        # List available collections
        collection_names = db.list_collection_names()
        logger.info(f"MongoDB collections: {', '.join(collection_names)}")
        
        # Check if we have the casts collection
        if 'casts' not in collection_names:
            logger.warning("Casts collection not found in MongoDB")
        
        return True
    except Exception as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        # Set these to None to indicate they're not available
        mongo_client = None
        async_mongo_client = None
        db = None
        async_db = None
        return False
# Call the local init function
init_mongodb()

FARSTORE_PASS = os.getenv('FARSTORE_PASS')
# Neo4j Configuration
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEYNAR_API_KEY = os.getenv("NEYNAR_API_KEY")

NEO4J_DATABASE = None

# Global Neo4j driver variable
neo4j_driver = None

def init_neo4j():
    """Initialize Neo4j driver connection."""
    global neo4j_driver
    
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
        
        return True
    except Exception as e:
        logger.error(f"Neo4j connection error: {str(e)}")
        # Set neo4j_driver to None to indicate it's not available
        neo4j_driver = None
        logger.warning("Neo4j driver is not available - API will run in limited mode")
        return False

# Call the init function
init_neo4j()

def execute_cypher(query, params=None):
    """Execute a Cypher query in Neo4j"""
    global neo4j_driver  # Explicitly use the global variable
    
    if neo4j_driver is None:
        logger.error("Neo4j driver is not initialized - cannot execute query")
        return []  # Return empty results instead of raising exception
        
    try:
        # Using None for database parameter will use the default database
        with neo4j_driver.session(database=NEO4J_DATABASE) as session:
            result = session.run(query, params)
            return list(result)
    except Exception as e:
        logger.error(f"Neo4j query execution error: {str(e)}")
        return []  # Return empty results on error

async def search_casts(query: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Search for casts using MongoDB Atlas Search
    
    Args:
        query: The search query to execute
        limit: Maximum number of results to return
        
    Returns:
        List of matching cast documents
    """
    try:
        # Check if MongoDB is available
        if async_db is None:
            logger.warning("MongoDB is not available for search, returning empty results")
            return []
            
        logger.info(f"Searching MongoDB for casts with query: '{query}', limit: {limit}")
        
        # Use Atlas Search query with text search operator
        search_pipeline = [
            {
                "$search": {
                    "index": "default",  # Using the default text index
                    "text": {
                        "query": query,
                        "path": ["text", "author", "mentionedUsernames"],  # Fields to search
                        "fuzzy": {
                            "maxEdits": 2,
                            "prefixLength": 1
                        }
                    }
                }
            },
            {
                "$limit": limit
            },
            # Add search score
            {
                "$addFields": {
                    "score": {
                        "$meta": "searchScore"
                    }
                }
            }
        ]
        
        # Execute the search pipeline
        results = await async_db.casts.aggregate(search_pipeline).to_list(length=limit)
        
        logger.info(f"MongoDB search returned {len(results)} cast results")
        
        # Convert MongoDB _id to string
        for doc in results:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        
        return results
    
    except Exception as e:
        logger.error(f"MongoDB search error: {str(e)}")
        logger.exception("MongoDB search exception:")
        return []

async def get_casts_by_hashes(hashes: List[str]) -> List[Dict[str, Any]]:
    """
    Retrieve casts by their hash values
    
    Args:
        hashes: List of cast hash values to retrieve
        
    Returns:
        List of matching cast documents
    """
    try:
        logger.info(f"Retrieving {len(hashes)} casts from MongoDB by hash")
        
        if not hashes:
            return []
            
        # Query casts by hash
        results = await async_db.casts.find({"hash": {"$in": hashes}}).to_list(length=len(hashes))
        
        logger.info(f"Retrieved {len(results)} casts from MongoDB by hash")
        
        # Convert MongoDB _id to string
        for doc in results:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
                
        return results
        
    except Exception as e:
        logger.error(f"Error retrieving casts by hash: {str(e)}")
        logger.exception("MongoDB get_casts_by_hashes exception:")
        return []
    

# Initialize FastAPI
app = FastAPI(title="Token API", description="API for querying token data from Neo4j")


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
    avgBalance: Optional[float] = Field(None, description = "Average balance held")
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
    balance: float = Field(..., description="Estimated balance of token held by believer, across Farcaster-linked wallets.")


    
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
    - Returns normalized believer scores (0-70) with detailed metrics
    - Includes market cap adjustments, token concentration, and social metrics
    - Provides raw and adjusted scores for transparency
    - Optionally filter by a specific token address
    """
    # Validate API key
    if request.api_key != os.getenv("CLANK_PASS"):
        raise HTTPException(status_code=401, detail="Invalid API key")
        
    try:
        # Prepare parameters
        params = {}
        # Build the query based on whether a token address is provided
        if request.token_address:
            # If token_address is provided, add filter to the query
            query = """
            MATCH (token:Token)
            WHERE toLower(token.address) = toLower($token_address)
            RETURN DISTINCT
                token.address as address, 
                token.name as name,
                token.symbol as symbol,
                token.believerScore as believerScore,
                token.rawBelieverScore as rawBelieverScore,
                token.diversityAdjustedScore as diversityAdjustedScore,
                token.marketAdjustedScore as marketAdjustedScore,
                token.holderToMarketCapRatio as holderToMarketCapRatio,
                token.marketCap as marketCap,
                token.walletCount as walletCount,
                token.warpcastWallets as warpcastWallets,
                token.warpcastPercentage as warpcastPercentage,
                token.avgSocialCredScore as avgSocialCredScore,
                token.totalSupply as totalSupply
            """
            params["token_address"] = request.token_address.lower()
        else:
            # If no token_address, return all tokens
            query = """
            MATCH (token:Token)
            RETURN DISTINCT
                token.address as address, 
                token.name as name,
                token.symbol as symbol,
                token.believerScore as believerScore,
                token.rawBelieverScore as rawBelieverScore,
                token.diversityAdjustedScore as diversityAdjustedScore,
                token.marketAdjustedScore as marketAdjustedScore,
                token.holderToMarketCapRatio as holderToMarketCapRatio,
                token.marketCap as marketCap,
                token.walletCount as walletCount,
                token.warpcastWallets as warpcastWallets,
                token.warpcastPercentage as warpcastPercentage,
                token.avgSocialCredScore as avgSocialCredScore,
                token.totalSupply as totalSupply
            ORDER BY token.believerScore DESC
            """
        
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
        MATCH (believerWallet)-[:ACCOUNT]-(wc:Warpcast:Account)  
        WHERE wc.fcCredScore is not null       
        ORDER BY wc.fcCredScore DESC LIMIT 25
        WITH wc, sum(tofloat(r.balance)) as balance
        RETURN {
            top_believers: COLLECT(DISTINCT({
                fid: tointeger(wc.fid),
                username: wc.username,
                bio: wc.bio,
                balance: balance,
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
    Get matching casts and related metadata using a hybrid MongoDB Atlas Search + Neo4j approach.
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
            os.makedirs("data/query_results", exist_ok=True)
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            clean_filename = clean_query_for_lucene(request.query) or "empty_query"
            json_filename = f"data/query_results/{clean_filename}_{timestamp_str}.json"
            
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump({
                    "query": request.query,
                    "timestamp": datetime.now().isoformat(),
                    "mongo_count": len(mongo_casts),
                    "enriched_mongo_count": len([c for c in combined_casts if c.get("source") == "mongo_enriched"]),
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
    if neo4j_driver is not None:
        neo4j_driver.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)