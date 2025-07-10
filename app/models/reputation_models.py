# /app/models/reputation_models.py
"""
Pydantic models for reputation-related endpoints.
"""
from pydantic import BaseModel, Field, validator
from typing import Dict, Optional, List

class ReputationData(BaseModel):
    """Model for Farcaster reputation data."""
    fid: int = Field(..., description="Farcaster user ID")
    username: str = Field(..., description="Farcaster username")
    quotientScore: Optional[float] = Field(None, description="Normalized quotient score - use for display to users. Account quality drops signifigantly beneath .5")
    quotientScoreRaw: Optional[float] = Field(None, description="Raw quotient score - use for rewards multipliers.")
    quotientRank: Optional[int] = Field(None, description="Account rank across Farcaster based on Quotient score.")
    quotientProfileUrl: str = Field(..., description="Review reach, engagement, and influence insights for the user in the Quotient discovery portal.")

class ReputationResponse(BaseModel):
    """Response model for reputation endpoint."""
    data: List[ReputationData] = Field(..., description="List of reputation data for the requested FIDs")
    count: int = Field(..., description="Number of users found")

class ReputationRequest(BaseModel):
    """Request model for reputation endpoint."""
    fids: List[int] = Field(..., description="List of Farcaster IDs (FIDs) to retrieve reputation for", max_items=100)
    api_key: str = Field(..., description="API key for authentication")
    
    @validator('fids')
    def validate_fids_length(cls, v):
        if len(v) == 0:
            raise ValueError('At least one FID must be provided')
        if len(v) > 100:
            raise ValueError('Maximum 100 FIDs allowed per request')
        return v