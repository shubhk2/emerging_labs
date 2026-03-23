from fastapi import APIRouter, HTTPException
from backend.services.home_service import get_dashboard_stats

router = APIRouter()

@router.get("")
async def home():
    """Home page with dashboard overview"""
    try:
        stats = get_dashboard_stats()
        return {
            "message": "Welcome to Financial Dashboard",
            "stats": stats,
            "version": "1.0.0"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
