from fastapi import APIRouter, Query, HTTPException
from backend.services.annual_files_service import map, get_ticker_from_company_number

router = APIRouter()

@router.get("/all")
async def get_annual_files(company_number: int = Query(..., description="Company number")):
    """
    Returns a list of annual report file IDs for the given company_number.
    """
    try:
        ticker = get_ticker_from_company_number(company_number)
        if not ticker:
            raise HTTPException(status_code=404, detail="Company number not found.")
        file_ids = map.get(ticker)
        if not file_ids:
            raise HTTPException(status_code=404, detail="No annual files found for this company.")
        return {"company_number": company_number, "ticker": ticker, "annual_file_ids": file_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/yearly")
async def get_annual_files_by_year(
    company_number: int = Query(..., description="Company number"),
    year: int = Query(..., description="Year of the annual report (2022, 2023, 2024)")
):
    """
    Returns the annual report file ID for the given company_number and year.
    """
    try:
        ticker = get_ticker_from_company_number(company_number)
        if not ticker:
            raise HTTPException(status_code=404, detail="Company number not found.")
        file_ids = map.get(ticker)
        if not file_ids:
            raise HTTPException(status_code=404, detail="No annual files found for this company.")
        # Map year to index: 2022->0, 2023->1, 2024->2
        year_to_index = {2022: 0, 2023: 1, 2024: 2}
        idx = year_to_index.get(year)
        if idx is None or idx >= len(file_ids):
            raise HTTPException(status_code=404, detail="Annual file for the requested year not available.")
        return {"company_number": company_number, "ticker": ticker, "year": year, "annual_file_id": file_ids[idx]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
