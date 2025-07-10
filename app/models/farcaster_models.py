# /app/models/farcaster_models.py
"""
Pydantic models for Farcaster-related endpoints.
"""
from pydantic import BaseModel, Field
from typing import List

class UserProfile(BaseModel):
    """Model for basic user profile information."""
    fid: int = Field(..., description="Farcaster user ID")
    username: str = Field(..., description="Farcaster username")
    pfp_url: str = Field(..., description="Profile picture URL")

class MutualsResponse(BaseModel):
    """Response model for mutual followers endpoint."""
    fid: int = Field(..., description="The FID that was queried for mutual followers")
    mutual_followers: List[UserProfile] = Field(..., description="List of users who mutually follow the queried FID")
    count: int = Field(..., description="Total count of mutual followers")