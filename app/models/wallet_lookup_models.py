# /app/models/wallet_lookup_models.py
"""
Pydantic models for wallet lookup endpoint.
"""
from pydantic import BaseModel, Field
from typing import List, Optional


class WalletLookupRequest(BaseModel):
    """Request model for wallet lookup by X handle."""
    handle: str = Field(..., description="X (Twitter) handle to look up wallets for")
    api_key: str = Field(..., description="API key for authentication")


class WalletLookupResponse(BaseModel):
    """Response model for wallet lookup endpoint."""
    handle: str = Field(..., description="The X handle that was queried")
    addresses: List[str] = Field(..., description="List of Ethereum wallet addresses (0x prefixed)")
    count: int = Field(..., description="Number of wallets found")