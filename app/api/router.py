# /app/api/router.py
"""
API router that includes all endpoint routers.
"""
from fastapi import APIRouter
from app.api.endpoints import (
    tokens, 
    reputation, 
    farcaster_users, 
    farcaster_connections,
    farcaster_connections_all,
    clankers, 
    allowlist, 
    leaderboard
)

# Create main router
router = APIRouter()

# Include all endpoint routers
router.include_router(tokens.router, tags=["Tokens"])
router.include_router(reputation.router, tags=["Reputation"])
router.include_router(farcaster_users.router, tags=["Farcaster Users"])
router.include_router(farcaster_connections.router, tags=["Farcaster Connections"])
router.include_router(farcaster_connections_all.router, tags=["Farcaster Connections"])
router.include_router(clankers.router, tags=["Clankers"])
router.include_router(allowlist.router, tags=["Allowlist"])
router.include_router(leaderboard.router, tags=["Leaderboards"])