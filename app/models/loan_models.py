# /app/models/loan_models.py
"""
Pydantic models for loan history endpoint.
"""
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class LoanHistoryRequest(BaseModel):
    """Request model for loan history lookup."""
    fid: Optional[int] = Field(None, description="Farcaster ID to look up loans for")
    fids: Optional[List[int]] = Field(None, description="List of Farcaster IDs (max 100)")
    api_key: str = Field(..., description="API key for authentication")


class Loan(BaseModel):
    """Individual loan record."""
    loan_id: str = Field(..., description="Unique loan identifier (origin tx hash)")
    fid: int = Field(..., description="Farcaster ID of borrower")
    borrower: str = Field(..., description="Ethereum address of borrower")
    principal_usdc: float = Field(..., description="Original loan amount in USDC")
    total_repaid_usdc: float = Field(..., description="Total amount repaid in USDC")
    remaining_usdc: float = Field(..., description="Remaining balance in USDC")
    loan_status: str = Field(..., description="FULLY_REPAID, ACTIVE_REPAYING, or ACTIVE_NO_PAYMENT")
    originated_at: Optional[str] = Field(None, description="Loan origination timestamp")
    last_repayment_at: Optional[str] = Field(None, description="Last repayment timestamp")
    fully_repaid_at: Optional[str] = Field(None, description="Full repayment timestamp")
    repayment_count: int = Field(..., description="Number of repayments made")


class LoanHistoryResponse(BaseModel):
    """Response model for loan history endpoint."""
    loans: List[Loan] = Field(..., description="List of loans")
    count: int = Field(..., description="Number of loans returned")
