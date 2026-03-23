from fastapi import APIRouter, Query, HTTPException
from backend.services.search_service import search_companies, search_parameters,search_company_by_id
import logging

# Set up logger
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["search"])

@router.get("/company/{company_id}")
async def get_company_by_id(
    company_id: int
):
    logger.debug(f"Searching company with ID: {company_id}")
    try:
        result = search_company_by_id(company_id)
        if not result:
            logger.info(f"No company found with ID: {company_id}")
            return {"result": None}
        logger.info(f"Company found: {result}")
        return {"result": result}
    except Exception as e:
        logger.error(f"Error fetching company by ID: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500,detail=str(e))

@router.get("/companies")
async def search_companies_route(
    q: str = Query(..., min_length=1, description="Search term for company name"),
    limit: int = Query(10, ge=1, le=50)
):
    logger.debug(f"Searching companies with query: '{q}', limit: {limit}")
    try:
        results = search_companies(q, limit)
        logger.info(f"Company search returned {len(results)} results")
        return {"results": results}
    except Exception as e:
        logger.error(f"Error in company search: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/parameters")
async def search_parameters_route(
    q: str = Query(..., min_length=1, description="Search term for parameter"),
    limit: int = Query(10, ge=1, le=50)
):
    logger.debug(f"Searching parameters with query: '{q}', limit: {limit}")
    try:
        results = search_parameters(q, limit)
        logger.info(f"Parameter search returned {len(results)} results")
        return {"results": results}
    except Exception as e:
        logger.error(f"Error in parameter search: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
