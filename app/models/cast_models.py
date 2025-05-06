"""
Pydantic models for cast-related endpoints.
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class CastRequest(BaseModel):
    """Request model for cast search."""
    query: str
    start_timestamp: Optional[int] = None
    end_timestamp: Optional[int] = None

class PaginationInfo(BaseModel):
    """Pagination metadata for cast search results."""
    count: int
    first_timestamp: Optional[str] = None
    last_timestamp: Optional[str] = None
    next_cursor: Optional[str] = None

class CastResponseData(BaseModel):
    """Response model for cast search."""
    casts: List[Dict]
    pagination: PaginationInfo

class RecentCast(BaseModel):
    """Model for a recent cast."""
    text: str = Field(..., description="Cast content")
    hash: str = Field(..., description="Unique cast identifier")
    timestamp: str = Field(..., description="Cast creation timestamp")

class CastData(BaseModel):
    """Model for detailed cast data with author info."""
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
    """Model for cast collection metrics."""
    casts: int = Field(..., description="Total matching casts")
    uniqueAuthors: int = Field(..., description="Distinct cast authors")
    rawWeightedScore: float = Field(..., description="Unmodified credibility score")
    diversityMultiplier: float = Field(..., description="Author diversity coefficient - penalizes spammers")
    weighted_score: float = Field(..., description="Final credibility score")
    
    class Config:
        extra = "allow"  # Allow extra fields

class WeightedCastsResponseData(BaseModel):
    """Response model for weighted casts search."""
    casts: List[Dict[str, Any]] = Field(..., description="Matching casts")
    total: int = Field(..., description="Total cast count")
    metrics: Dict[str, Any] = Field(..., description="Cast collection metrics")
    
    class Config:
        extra = "allow"  # Allow extra fields that may be returned by the API