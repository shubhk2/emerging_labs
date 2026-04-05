from fastapi import APIRouter, HTTPException
from backend.services.overview_service import get_company_overview, get_company_stats
from fastapi.concurrency import run_in_threadpool

router = APIRouter()

@router.get("/company/{company_number}")
async def company_overview(company_number: int):
    """
    Get textual overview and stats for a company
    """
    try:
        # Wrap blocking calls in threadpool
        overview = await run_in_threadpool(get_company_overview, company_number)
        stats = await run_in_threadpool(get_company_stats, company_number)
        
        return {
            "overview": overview,
            "stats": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
