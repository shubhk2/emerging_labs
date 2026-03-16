from fastapi import APIRouter, HTTPException, Query
from backend.services.cg_committee_meetings_service import get_cg_committee_meetings

router = APIRouter()


@router.get("")
async def get_committee_meetings(company_number: int = Query(..., description="Company number")):
    """
    Returns committee meetings details for the given company_number.
    Excludes 'id' and 'company_no' columns.
    """
    result = get_cg_committee_meetings(company_number)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

