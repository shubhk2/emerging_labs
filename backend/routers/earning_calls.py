from fastapi import APIRouter, HTTPException, Query
from backend.services.earning_calls_service import get_earning_call_file
from fastapi.concurrency import run_in_threadpool

router = APIRouter()

@router.get("/earning_calls_files")
async def earning_calls_files(company_number: int = Query(...), quarter: int = Query(...), year: int = 2025):
    """
    Fetch the earnings call document for a company by company_number, quarter, and year (default 2025).
    """
    try:
        result = await run_in_threadpool(get_earning_call_file,company_number, quarter, year)
        if not result:
            raise HTTPException(status_code=404, detail="Earnings call not found.")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

