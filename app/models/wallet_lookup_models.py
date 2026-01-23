# /app/models/wallet_lookup_models.py
"""
Pydantic models for wallet lookup endpoint.
"""
from pydantic import BaseModel, Field, validator
from typing import List


class WalletLookupRequest(BaseModel):
    """Request model for wallet lookup by social username."""
    username: str = Field(..., description="Username to look up wallets for")
    platform: str = Field(..., description="Platform: 'farcaster' or 'x'")
    api_key: str = Field(..., description="API key for authentication")
    
    @validator('platform')
    def validate_platform(cls, v):
        allowed = ['farcaster', 'x']
        if v.lower() not in allowed:
            raise ValueError(f"Platform must be one of: {allowed}")
        return v.lower()


class WalletLookupResponse(BaseModel):
    """Response model for wallet lookup endpoint."""
    username: str = Field(..., description="The username that was queried")
    platform: str = Field(..., description="The platform that was queried")
    addresses: List[str] = Field(..., description="List of Ethereum wallet addresses (0x prefixed)")
    count: int = Field(..., description="Number of wallets found")