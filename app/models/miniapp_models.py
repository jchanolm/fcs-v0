"""
Pydantic models for miniapp-related endpoints.
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class MiniappMentionData(BaseModel):
    """Model for basic miniapp mention data."""
    name: str = Field(..., description="Miniapp name")
    frameUrl: str = Field(..., description="Frame URL")
    mentions: int = Field(..., description="Number of mentions")
    fcsWeightedMentions: float = Field(..., description="FCS weighted mentions")

class MiniappMention(BaseModel):
    """Model for detailed miniapp mention data."""
    name: str
    frameUrl: str
    mentionsAllTime: Optional[float] = 0.0
    uniqueCasters: Optional[int] = 0
    rawWeightedCasts: Optional[float] = 0.0
    weightedCasts: Optional[float] = 0.0
    avgFcsCredScore: Optional[float] = 0.0
    
    class Config:
        extra = "allow"  # Allow extra fields

class MiniappMentionsData(BaseModel):
    """Container for miniapp mentions data."""
    mentions: List[Dict[str, Any]]
    
    class Config:
        extra = "allow"  # Allow extra fields

class MiniappMentionsResponse(BaseModel):
    """Response model for miniapp mentions endpoint."""
    data: Dict[str, Any]
    
    class Config:
        extra = "allow"  # Allow extra fields

class KeyPromotersRequest(BaseModel):
    """Request model for key promoters endpoint."""
    miniapp_name: str = Field(..., description="Name of the miniapp to retrieve key promoters for")

class Promoter(BaseModel):
    """Model for a miniapp promoter."""
    username: str = Field(..., description="Social media username")
    fid: int = Field(..., description="Farcaster user identifier")
    fcCredScore: float = Field(..., description="Farcaster credibility score")
    recentCasts: List[Dict[str, Any]] = Field(..., description="Recent user posts")
    
    class Config:
        extra = "allow"  # Allow extra fields

class KeyPromotersData(BaseModel):
    """Response model for key promoters endpoint."""
    promoters: List[Dict[str, Any]]
    
    class Config:
        extra = "allow"  # Allow extra fields