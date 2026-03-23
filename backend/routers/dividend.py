from fastapi import APIRouter, HTTPException, Query
from backend.services.dividend_service import get_dividend_data
from fastapi.concurrency import run_in_threadpool

router = APIRouter()

@router.get("")
async def get_dividend(company_number: int = Query(..., description="Company number")):
    """
    Returns a string representing the file id for the given company_number.
    """
    try:
        data = await run_in_threadpool(
            get_dividend_data,
            company_number)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
