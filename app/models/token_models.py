"""
Pydantic models for token-related endpoints.
"""
from pydantic import BaseModel, Field, root_validator
from typing import List, Dict, Any, Optional, Union

class TokensRequest(BaseModel):
    """Request model for token believer score endpoint."""
    api_key: str = Field(..., description="API key for authentication")
    token_address: Optional[str] = None

class TokenData(BaseModel):
    """Model for token data with believer scores."""
    address: str = Field(..., description="Token contract address")
    name: Optional[str] = Field(None, description="Token name")
    symbol: Optional[str] = Field(None, description="Token $symbol")
    believerScore: Optional[float] = Field(None, description="Normalized believer score (0-100)")
    rawBelieverScore: Optional[float] = Field(None, description="Raw believer score before adjustments")
    diversityAdjustedScore: Optional[float] = Field(None, description="Believer score adjusted for token concentration")
    marketAdjustedScore: Optional[float] = Field(None, description="Believer score adjusted for market cap ratio")
    holderToMarketCapRatio: Optional[float] = Field(None, description="Ratio of holders to market cap")
    avgBalance: Optional[float] = Field(None, description="Average balance held")
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
    """Response model for token believer score endpoint."""
    fcs_data: List[TokenData] = Field(..., description="List of token data with believer scores")

class BelieversDataRequest(BaseModel):
    """Request model for top believers endpoint."""
    token_address: str = Field(..., description="Token contract address")
    
class TopBelieversData(BaseModel):
    """Model for individual token believer data."""
    fid: int = Field(..., description="User Farcaster ID.")
    username: str = Field(..., description="User Farcaster username.")
    bio: str = Field(..., description="User Farcaster Bio.")
    pfpUrl: str = Field(..., description="PFP URL for user.")
    fcred: float = Field(..., description="User Farcaster Cred Score (i.e. Social Cred Score).")
    balance: float = Field(..., description="Estimated balance of token held by believer, across Farcaster-linked wallets.")
    
    class Config:
        extra = "allow"  # Allow extra fields

class PaginationInfo(BaseModel):
    """Pagination metadata for search results."""
    count: int
    first_timestamp: Optional[str] = None
    last_timestamp: Optional[str] = None
    next_cursor: Optional[str] = None  # Added next_cursor to the model

class RecentCast(BaseModel):
    """Model for a recent cast."""
    text: str = Field(..., description="Cast content")
    hash: str = Field(..., description="Unique cast identifier")
    timestamp: str = Field(..., description="Cast creation timestamp")