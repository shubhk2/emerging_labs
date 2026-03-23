from fastapi import APIRouter, HTTPException, Query
from backend.services.insider_trading_service import get_insider_trading_data

router = APIRouter()

@router.get("")
async def get_insider_trading(company_number: int = Query(..., description="Company number")):
    """
    Returns insider trading/pledged data for the given company_number.
    """
    result = get_insider_trading_data(company_number)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

