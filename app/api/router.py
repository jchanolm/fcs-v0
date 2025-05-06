"""
API router that includes all endpoint routers.
"""
from fastapi import APIRouter
from app.api.endpoints import tokens, miniapps, casts

# Create main router
router = APIRouter()

# Include all endpoint routers
router.include_router(tokens.router, tags=["Tokens"])
router.include_router(miniapps.router, tags=["Farstore"])
router.include_router(casts.router, tags=["Search"])