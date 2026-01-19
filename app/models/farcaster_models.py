# /app/models/farcaster_models.py
"""
Pydantic models for Farcaster-related endpoints.
"""
from pydantic import BaseModel, Field
from typing import List, Optional

class UserProfile(BaseModel):
    """Model for basic user profile information."""
    fid: int = Field(..., description="Farcaster user ID")
    username: str = Field(..., description="Farcaster username")
    pfp_url: str = Field(..., description="Profile picture URL")

class MutualsRequest(BaseModel):
    """Request model for mutual followers endpoint."""
    fid: int = Field(..., description="Farcaster ID (FID) to find mutual followers for")
    api_key: str = Field(..., description="API key for authentication")

class MutualsResponse(BaseModel):
    """Response model for mutual followers endpoint."""
    fid: int = Field(..., description="The FID that was queried for mutual followers")
    mutual_followers: List[UserProfile] = Field(..., description="List of users who mutually follow the queried FID")
    count: int = Field(..., description="Total count of mutual followers")


class LinkedWalletsRequest(BaseModel):
    """Request model for linked wallets endpoint."""
    wallet_address: str = Field(..., description="Ethereum wallet address to look up")
    api_key: str = Field(..., description="API key for authentication")


class LinkedWalletsResponse(BaseModel):
    """Response model for linked wallets endpoint."""
    input_address: str = Field(..., description="The queried wallet address (normalized to lowercase)")
    fid: Optional[int] = Field(None, description="Farcaster ID associated with the wallet, if found")
    username: Optional[str] = Field(None, description="Farcaster username associated with the wallet, if found")
    linked_wallets: List[str] = Field(..., description="All verified wallet addresses linked to this Farcaster account")
    count: int = Field(..., description="Number of linked wallets found")