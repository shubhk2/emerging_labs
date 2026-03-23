from fastapi import APIRouter, HTTPException, Query
from backend.services.pledged_data_service import get_pledged_data

router = APIRouter()

@router.get("")
async def get_pledged(company_number: int = Query(..., description="Company number")):
    """
    Returns insider trading/pledged data for the given company_number.
    """
    result = get_pledged_data(company_number)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

