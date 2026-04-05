from fastapi import HTTPException, APIRouter, Query,Path
from backend.services.stock_data_service import create_stock_chart, get_stock_data_table
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/{data_type}/{period}/chart")
async def get_stock_chart(
    company_number: int = Query(..., description="Company number"),
    data_type: str = Path(..., enum=["price", "dma50", "dma200"]),
    period: str = Path(..., enum=["1month", "6month", "1yr", "3yr", "5yr", "10yr"])
):
    """
    Get a Plotly chart (JSON) for the selected stock data type and period.
    """
    try:
        result = create_stock_chart(company_number, data_type, period)
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    except Exception as e:
        logger.error(f"Error generating stock chart: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{data_type}/{period}/table")
async def get_stock_table(
    company_number: int = Query(..., description="Company number"),
    data_type: str = Path(..., enum=["price", "dma50", "dma200"]),
    period: str = Path(..., enum=["1month", "6month", "1yr", "3yr", "5yr", "10yr"])
):
    """
    Get the raw data table for the selected stock data type and period.
    """
    try:
        result = get_stock_data_table(company_number, data_type, period)
        return result
    except Exception as e:
        logger.error(f"Error fetching stock data table: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
