# /app/api/router.py
"""
API router that includes all endpoint routers.
"""
from fastapi import APIRouter
from app.api.endpoints import tokens, miniapps, casts, reputation, farcaster_users, clankers

# Create main router
router = APIRouter()

# Include all endpoint routers
router.include_router(tokens.router, tags=["Tokens"])
# router.include_router(miniapps.router, tags=["Farstore"])
# router.include_router(casts.router, tags=["Search"])
router.include_router(reputation.router, tags=["Reputation"])
router.include_router(farcaster_users.router, tags=["Farcaster Users"])
router.include_router(clankers.router, tags=["Clankers"])