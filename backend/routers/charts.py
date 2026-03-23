from fastapi import APIRouter, HTTPException
from typing import Optional, List
from pydantic import BaseModel
from backend.services.chart_service import generate_parameter_chart, generate_ratio_chart
import logging
from fastapi.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)
router = APIRouter()


class ChartRequest(BaseModel):
    company_numbers: List[int]
    parameters: List[str]
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    chart_type: str = "line"


@router.post("/parameters")
async def chart_parameters(request: ChartRequest):
    """Generate a chart for specified financial parameters."""
    logger.info(f"Chart parameters request received: {request.dict()}")
    try:
        if not request.company_numbers or not request.parameters:
            raise HTTPException(status_code=400, detail="Company numbers and parameters must be provided.")

        chart_data = await run_in_threadpool(
            generate_parameter_chart,
            request.company_numbers,
            request.parameters,
            request.start_year,
            request.end_year,
            request.chart_type
        )
        return chart_data
    except Exception as e:
        logger.error(f"Error generating parameter chart: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ratios")
async def chart_ratios(request: ChartRequest):
    """Generate a chart for specified financial ratios."""
    logger.info(f"Chart ratios request received: {request.model_dump()}")
    try:
        if not request.company_numbers or not request.parameters:
            raise HTTPException(status_code=400, detail="Company numbers and ratios must be provided.")

        chart_data = await run_in_threadpool(
            generate_ratio_chart,
            request.company_numbers,
            request.parameters,
            request.start_year,
            request.end_year,
            request.chart_type
        )
        return chart_data
    except Exception as e:
        logger.error(f"Error generating ratio chart: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))