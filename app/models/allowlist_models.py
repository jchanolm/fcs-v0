# app/models/allowlist_models.py
"""
Pydantic models for allowlist-related endpoints.
"""
from pydantic import BaseModel, Field
from typing import List, Optional

class UserEligibilityData(BaseModel):
    """Model for user eligibility data."""
    fid: int = Field(..., description="Farcaster user ID")
    username: str = Field(..., description="Farcaster username")
    pfp_url: Optional[str] = Field(None, description="Profile picture URL")
    quotient_score: float = Field(..., description="User's quotient score")
    quotient_rank: Optional[int] = Field(None, description="User's quotient rank")
    primary_eth_address: Optional[str] = Field(None, description="Primary Ethereum address")
    eligible: bool = Field(True, description="Whether user meets all allowlist criteria")

class ConditionResult(BaseModel):
    """Model for individual condition check result."""
    type: str = Field(..., description="Type of condition (e.g., 'farcaster-follower')")
    target_name: str = Field(..., description="Target name for the condition")
    meets_condition: bool = Field(..., description="Whether user meets this specific condition")

class CheckResponse(BaseModel):
    """Response model for single user eligibility check."""
    fid: int = Field(..., description="Farcaster user ID that was checked")
    username: Optional[str] = Field(None, description="Farcaster username")
    eligible: bool = Field(..., description="Overall eligibility status")
    quotient_score: float = Field(..., description="User's quotient score")
    meets_reputation_threshold: bool = Field(..., description="Whether user meets reputation requirement")
    conditions: List[ConditionResult] = Field(..., description="Results for each allowlist condition")
    primary_eth_address: Optional[str] = Field(None, description="Primary Ethereum address")
    request_count: Optional[int] = Field(None, description="Updated request count for this allowlist")

class UsersResponse(BaseModel):
    """Response model for all eligible users endpoint."""
    users: List[UserEligibilityData] = Field(..., description="List of all eligible users")
    total_count: int = Field(..., description="Total number of eligible users")
    request_count: Optional[int] = Field(None, description="Updated request count for this allowlist")