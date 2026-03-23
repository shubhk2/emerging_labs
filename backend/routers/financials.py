from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from pydantic import BaseModel  # Import BaseModel
from backend.services.financial_service import (
    get_financial_data,
    get_financial_periods,
    get_financial_data_by_parameters
)
from fastapi.concurrency import run_in_threadpool

router = APIRouter()


@router.get("")
async def get_financials(
        company_number: int,
        statement_type: str = Query(..., enum=["balance_sheet", "profit_and_loss", "cashflow"]),
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
):
    """
    Get financial statements for a company.

    The data is returned in a structured format suitable for table display.

    Parameters:
    - company_number: Company number (corresponds to company_detail.id).
    - statement_type: Type of financial statement.
    - start_year: Start year for data filter (e.g., 2018). Optional.
    - end_year: End year for data filter (e.g., 2022). Optional.
    """
    try:
        data = await run_in_threadpool(
            get_financial_data,
            company_number,
            statement_type,
            start_year,
            end_year
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/periods")
async def get_periods():
    """Get available time periods (years) for all financial statements."""
    try:
        periods = await run_in_threadpool(
            get_financial_periods
        )
        return {"periods": periods}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Define a Pydantic model for the request body
class FinancialsParamsBody(BaseModel):
    parameters: List[str]
    start_year: Optional[int] = None
    end_year: Optional[int] = None


@router.post("/parameters")
async def get_financials_by_parameters(
    company_number: int,
    body: FinancialsParamsBody,
    statement_type: str = Query(..., enum=["balance_sheet", "profit_and_loss", "cashflow"])
      # Use the Pydantic model for the body
):
    """
    Get select financial data where 'account' matches the given parameters.
    - company_number: The numeric ID of the company (in query string).
    - statement_type: "balance_sheet", "profit_and_loss", or "cashflow" (in query string).
    - The request BODY should be a JSON object with:
        - parameters: A list of specific accounts to retrieve (e.g. ["Revenue", "Net Profit"]).
        - start_year and end_year (optional).
    """
    try:
        data = await run_in_threadpool(
            get_financial_data_by_parameters,
            company_number,
            statement_type,
            body.parameters,  # Get parameters from the body
            body.start_year,   # Get start_year from the body
            body.end_year      # Get end_year from the body
        )
        return data
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
