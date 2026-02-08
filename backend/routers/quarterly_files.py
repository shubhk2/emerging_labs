from fastapi import APIRouter, Query, HTTPException
from backend.services.quartely_files_service import map, get_ticker_from_company_number

router = APIRouter()

@router.get("/all")
async def get_quarterly_files(company_number: int = Query(..., description="Company number")):
    """
    Returns a list of quarterly report file IDs for the given company_number.
    """
    try:
        ticker = get_ticker_from_company_number(company_number)
        if not ticker:
            raise HTTPException(status_code=404, detail="Company number not found.")
        file_ids = map.get(ticker)
        if not file_ids:
            raise HTTPException(status_code=404, detail="No quarterly files found for this company.")
        return {"company_number": company_number, "ticker": ticker, "quarterly_file_ids": file_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/quarterly")
async def get_quarterly_files_by_quarter(
    company_number: int = Query(..., description="Company number"),
    quarter: int = Query(..., description="Quarter of the financial year (1-4)")
):
    """
    Returns the quarterly report file ID for the given company_number and quarter.
    """
    try:
        ticker = get_ticker_from_company_number(company_number)
        if not ticker:
            raise HTTPException(status_code=404, detail="Company number not found.")
        file_ids = map.get(ticker)
        if not file_ids:
            raise HTTPException(status_code=404, detail="No quarterly files found for this company.")
        # Quarter mapping: last item is Q4, second last is Q3, etc.
        if quarter < 1 or quarter > 4 or quarter > len(file_ids):
            raise HTTPException(status_code=404, detail="Quarterly file for the requested quarter not available.")
        file_id = file_ids[-quarter]
        return {"company_number": company_number, "ticker": ticker, "quarter": quarter, "quarterly_file_id": file_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
