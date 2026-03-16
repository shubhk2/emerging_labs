from fastapi import APIRouter, HTTPException, Query
from backend.services.cg_board_meetings_service import get_cg_board_meetings

router = APIRouter()


@router.get("")
async def get_board_meetings(company_number: int = Query(..., description="Company number")):
    """
    Returns board meetings details for the given company_number.
    Excludes 'id' and 'company_no' columns.
    """
    result = get_cg_board_meetings(company_number)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

