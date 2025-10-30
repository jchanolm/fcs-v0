# /app/models/leaderboard_models.py
"""
Pydantic models for leaderboard-related endpoints.
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

class LeaderboardEntry(BaseModel):
    """Model for a single leaderboard entry."""
    # Using Dict to allow flexible column names from different leaderboards
    data: Dict[str, Any] = Field(..., description="Leaderboard entry data with dynamic columns")

class LeaderboardResponse(BaseModel):
    """Response model for full leaderboard endpoint."""
    leaderboard_name: str = Field(..., description="Name of the leaderboard")
    data: List[Dict[str, Any]] = Field(..., description="List of leaderboard entries")
    count: int = Field(..., description="Number of entries in the leaderboard")
    run_timestamp: Optional[datetime] = Field(None, description="Timestamp of the leaderboard run (None when run_timestamp=all)")
    run_timestamps: Optional[List[datetime]] = Field(None, description="List of all run timestamps included (only present when run_timestamp=all)")

class UserLeaderboardResponse(BaseModel):
    """Response model for individual user leaderboard lookup."""
    leaderboard_name: str = Field(..., description="Name of the leaderboard")
    user_identifier: str = Field(..., description="User identifier used for lookup (e.g., fid)")
    data: Optional[Any] = Field(None, description="User's leaderboard entry data (Dict for single entry, List[Dict] for all historical entries)")
    found: bool = Field(..., description="Whether the user was found in the leaderboard")
    run_timestamp: Optional[datetime] = Field(None, description="Timestamp of the leaderboard run (None when run_timestamp=all or not found)")
    run_timestamps: Optional[List[datetime]] = Field(None, description="List of all run timestamps included (only present when run_timestamp=all)")