from fastapi import APIRouter, HTTPException, Query
from backend.services.rpt_service import get_rpt_data

router = APIRouter()


@router.get("")
async def get_rpt(company_number: int = Query(..., description="Company number")):
    """
    Returns Related Party Transactions (RPT) details for the given company_number.
    Excludes 'id' and 'company_no' columns.
    """
    result = get_rpt_data(company_number)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

