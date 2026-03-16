from fastapi import APIRouter, HTTPException, Query
from backend.services.cg_board_composition_service import get_cg_board_composition

router = APIRouter()


@router.get("")
async def get_board_composition(company_number: int = Query(..., description="Company number")):
    """
    Returns board composition details for the given company_number.
    Excludes 'id' and 'company_no' columns.
    """
    result = get_cg_board_composition(company_number)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

