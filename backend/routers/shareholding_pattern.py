from fastapi import APIRouter, HTTPException, Query
from backend.services.shareholding_pattern_service import get_shareholding_data

router = APIRouter()

@router.get("")
async def get_shareholding_pattern(company_number: int = Query(..., description="Company number")):
    """
    Returns shareholding details for the given company_number, similar to /financials.
    Skips the 'context' column.
    """
    result = get_shareholding_data(company_number)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result
