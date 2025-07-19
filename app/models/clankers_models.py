# /app/models/clankers_models.py
"""
Pydantic models for clankers-related endpoints.
"""
from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional

class UserHolder(BaseModel):
    """Model for a user holding a token."""
    fid: int = Field(..., description="Farcaster user ID")
    username: str = Field(..., description="Farcaster username")
    pfpUrl: Optional[str] = Field(None, description="Profile picture URL")
    quotientScore: Optional[float] = Field(None, description="User's quotient score")

class TokenHoldingData(BaseModel):
    """Model for token holding information."""
    address: str = Field(..., description="Token contract address")
    name: Optional[str] = Field(None, description="Token name")
    description: Optional[str] = Field(None, description="Token description")
    imageUrl: Optional[str] = Field(None, description="Token image URL")
    count_holders: int = Field(..., description="Number of holders from the queried FIDs")
    holders: List[UserHolder] = Field(..., description="List of users holding this token")

class ClankersHoldsRequest(BaseModel):
    """Request model for clankers holds-tokens endpoint."""
    fids: List[int] = Field(..., description="List of Farcaster IDs (FIDs) to query token holdings for")
    api_key: str = Field(..., description="API key for authentication")
    chain: Optional[str] = Field("arbitrum", description="Blockchain to query (default: arbitrum)")
    
    @validator('fids')
    def validate_fids_length(cls, v):
        if len(v) == 0:
            raise ValueError('At least one FID must be provided')
        return v

class ClankersHoldsResponse(BaseModel):
    """Response model for clankers holds-tokens endpoint."""
    tokens: List[TokenHoldingData] = Field(..., description="List of tokens held by the queried users")
    total_tokens: int = Field(..., description="Total number of unique tokens found")
    queried_fids: int = Field(..., description="Number of FIDs queried")
    chain: str = Field(..., description="Blockchain queried")

# Legacy aliases for backwards compatibility
ClankersRequest = ClankersHoldsRequest
ClankersResponse = ClankersHoldsResponse