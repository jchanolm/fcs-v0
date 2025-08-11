# app/models/allowlist_models.py
"""
Pydantic models for allowlist-related endpoints.
"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Union
from enum import Enum

class AllowlistMode(str, Enum):
    """Valid modes for allowlist endpoint."""
    users = "users"
    check = "check"

class AllowlistCheckRequest(BaseModel):
    """Request model for allowlist check endpoint."""
    query_id: str = Field(..., description="Allowlist query ID")
    mode: AllowlistMode = Field(..., description="Either 'users' or 'check'")
    fid: Optional[int] = Field(None, description="Required when mode='check'")
    
    @validator('fid')
    def validate_fid_for_check_mode(cls, v, values):
        if values.get('mode') == AllowlistMode.check and v is None:
            raise ValueError('fid is required when mode is "check"')
        return v

class UserEligibilityData(BaseModel):
    """Model for user eligibility data."""
    fid: int = Field(..., description="Farcaster user ID")
    username: str = Field(..., description="Farcaster username")
    primaryEthAddress: str = Field(..., description="Farcaster primary ETH address"),
    pfp_url: Optional[str] = Field(None, description="Profile picture URL")
    quotient_score: float = Field(..., description="User's quotient score")
    quotient_rank: Optional[int] = Field(None, description="User's quotient rank")
    eligible: bool = Field(..., description="Whether user meets all allowlist criteria")

class ConditionResult(BaseModel):
    """Model for individual condition check result."""
    type: str = Field(..., description="Type of condition (e.g., 'farcaster-follower')")
    target_name: str = Field(..., description="Target name for the condition")
    meets_condition: bool = Field(..., description="Whether user meets this specific condition")

class CheckResponse(BaseModel):
    """Response model for single user check."""
    fid: int = Field(..., description="Farcaster user ID that was checked")
    eligible: bool = Field(..., description="Overall eligibility status")
    quotient_score: float = Field(..., description="User's quotient score")
    meets_reputation_threshold: bool = Field(..., description="Whether user meets reputation requirement")
    conditions: List[ConditionResult] = Field(..., description="Results for each allowlist condition")

class UsersResponse(BaseModel):
    """Response model for all eligible users."""
    users: List[UserEligibilityData] = Field(..., description="List of all eligible users")
    total_count: int = Field(..., description="Total number of eligible users")

class AllowlistCheckResponse(BaseModel):
    """Main response model for allowlist check endpoint."""
    query_id: str = Field(..., description="The allowlist query ID that was checked")
    mode: str = Field(..., description="The mode used ('users' or 'check')")
    request_count: int = Field(..., description="Updated request count for this allowlist")
    data: Union[CheckResponse, UsersResponse] = Field(..., description="Response data based on mode")
    
    class Config:
        extra = "allow"