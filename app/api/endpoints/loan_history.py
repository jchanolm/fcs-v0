# /app/api/endpoints/loan_history.py
"""
Loan history API endpoint - get Quotient loan data for Farcaster users.
"""
import logging
from fastapi import APIRouter, HTTPException
from app.models.loan_models import LoanHistoryRequest, LoanHistoryResponse, Loan
from app.db.postgres import execute_postgres_query
from app.config import REPUTATION_PASS
from typing import Dict, Any

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post(
    "/loan-history",
    summary="Get Quotient loan history for Farcaster users",
    description="Retrieves loan history including status, principal, repayments for up to 100 FIDs.",
    response_model=LoanHistoryResponse,
    responses={
        200: {"description": "Successfully retrieved loan history", "model": LoanHistoryResponse},
        401: {"description": "Unauthorized - Invalid API key"},
        404: {"description": "No loans found for the provided FIDs"},
        500: {"description": "Internal Server Error"}
    }
)
async def get_loan_history(request: LoanHistoryRequest) -> Dict[str, Any]:
    """
    Get loan history for Farcaster users.

    - Requires valid API key
    - Accepts single fid or list of fids (max 100)
    - Returns all loans with status, amounts, and timestamps
    """
    if request.api_key != REPUTATION_PASS:
        raise HTTPException(status_code=401, detail="Invalid API key")

    # Build fid list
    fids = []
    if request.fid:
        fids = [request.fid]
    elif request.fids:
        fids = request.fids[:100]  # Limit to 100
    else:
        raise HTTPException(status_code=400, detail="Must provide fid or fids")

    logger.info(f"POST /loan-history - Looking up loans for {len(fids)} FIDs")

    try:
        query = """
        SELECT
            loan_id,
            fid,
            borrower,
            principal_usdc,
            total_repaid_usdc,
            remaining_usdc,
            loan_status,
            originated_at,
            last_repayment_at,
            fully_repaid_at,
            repayment_count
        FROM quotient.loan_history
        WHERE fid = ANY(:fids)
        ORDER BY originated_at DESC
        """

        results = execute_postgres_query(query, {"fids": fids})

        if not results:
            return {
                "loans": [],
                "count": 0
            }

        loans = []
        for row in results:
            loans.append(Loan(
                loan_id=row["loan_id"],
                fid=int(row["fid"]),
                borrower=row["borrower"],
                principal_usdc=float(row["principal_usdc"]),
                total_repaid_usdc=float(row["total_repaid_usdc"]),
                remaining_usdc=float(row["remaining_usdc"]),
                loan_status=row["loan_status"],
                originated_at=str(row["originated_at"]) if row["originated_at"] else None,
                last_repayment_at=str(row["last_repayment_at"]) if row["last_repayment_at"] else None,
                fully_repaid_at=str(row["fully_repaid_at"]) if row["fully_repaid_at"] else None,
                repayment_count=row["repayment_count"]
            ))

        logger.info(f"Found {len(loans)} loans for requested FIDs")

        return {
            "loans": loans,
            "count": len(loans)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching loan history: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
