"""
Pydantic models for reputation-related endpoints.
"""
from pydantic import BaseModel, Field
from typing import Dict

class EngagedAccountsData(BaseModel):
    """Model for engagement metrics from other accounts."""
    total: int = Field(..., description="Total number of distinct accounts that engaged")
    replied: int = Field(..., description="Number of accounts that replied")
    recasted: int = Field(..., description="Number of accounts that recasted")
    liked: int = Field(..., description="Number of accounts that liked")
    followed: int = Field(..., description="Number of accounts that followed")

class ReputationData(BaseModel):
    """Model for Farcaster reputation data."""
    fcCredRank: int = Field(..., description="Farcaster Credibility Ranking")
    fcCredScore: float = Field(..., description="Farcaster Credibility - Raw Score")
    bridgeRank: int = Field(..., description="Ranked ability to bridge disconnected communities on Farcaster"),
    bridgeScore: float = Field(..., description="Raw score for bridgeRank")
    engagedQualityAccounts: EngagedAccountsData = Field(..., description="Engagement metrics from other accounts")

class ReputationResponse(BaseModel):
    """Response model for reputation endpoint."""
    data: ReputationData = Field(..., description="Reputation data for the requested FID")

class ReputationRequest(BaseModel):
    """Request model for reputation endpoint."""
    fid: int = Field(..., description="Farcaster ID (FID) to retrieve reputation for")
    api_key: str = Field(..., description="API key for authentication")