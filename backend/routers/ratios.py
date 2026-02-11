from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from pydantic import BaseModel  # Import BaseModel
from backend.services.ratio_service import get_predefined_ratios, get_ratios_by_parameters
from fastapi.concurrency import run_in_threadpool
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("")
async def get_ratios(
        company_numbers: List[int] = Query(...),
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
):
    """
    Get predefined financial ratios for one or more companies.

    The data is returned in a structured format suitable for table display,
    with one object per company.

    Parameters:
    - company_numbers: List of company numbers to fetch ratios for.
    - start_year: Start year for data filter (e.g., 2018). Optional.
    - end_year: End year for data filter (e.g., 2022). Optional.
    """
    try:
        ratios = await run_in_threadpool(
            get_predefined_ratios,
            company_numbers,
            start_year,
            end_year
        )
        if not ratios:
            raise HTTPException(status_code=404, detail="No ratio data found for the specified companies.")
        return ratios
    except Exception as e:
        logger.error(f"Error fetching ratios: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Define a Pydantic model for the request body
class RatiosParamsBody(BaseModel):
    company_numbers: List[int]
    parameters: List[str]
    start_year: Optional[int] = None
    end_year: Optional[int] = None


@router.post("/parameters")
async def get_ratios_by_parameters_endpoint(body: RatiosParamsBody):
    """
    Get specific financial ratios for one or more companies.

    The request BODY should be a JSON object with:
    - company_numbers: List of company numbers to fetch ratios for.
    - parameters: A list of specific ratio names to retrieve (e.g. ["ROCE", "Debt to equity"]).
    - start_year: Start year for data filter. Optional.
    - end_year: End year for data filter. Optional.
    """
    try:
        ratios = await run_in_threadpool(
            get_ratios_by_parameters,
            body.company_numbers,
            body.parameters,
            body.start_year,
            body.end_year
        )
        if not ratios or not any(r['data'] for r in ratios):
            raise HTTPException(status_code=404, detail="No ratio data found for the specified companies and parameters.")
        return ratios
    except Exception as e:
        logger.error(f"Error fetching ratios by parameters: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
