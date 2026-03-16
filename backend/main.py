import logging
logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
from fastapi import FastAPI, Depends
import os
import asyncio
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

import uvloop
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from fastapi.responses import HTMLResponse

# Import your security module
from backend.services.security import get_api_key,get_api_key_docs

# Configure logging


logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Import routers
from backend.routers import stock_data, home, financials, ratios, overview, charts
from backend.routers import search, dividend, shareholding_pattern, sql_rag
from backend.routers import annual_files, quarterly_files, earning_calls
from backend.routers import insider_trading
from backend.routers import pledged_data
from backend.routers import cg_board_composition, cg_committee_composition, cg_board_meetings
from backend.routers import rpt, cg_committee_meetings



@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on startup
    logger.info("Configuring application startup settings...")
    try:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("✅ Using uvloop for enhanced async performance")
    except ImportError:
        logger.info("⚠️ uvloop not available, using default asyncio event loop")

    # Configure thread pool for database operations
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(
        max_workers=50,
        thread_name_prefix="fastapi-db-"
    )
    loop.set_default_executor(executor)
    logger.info(f"✅ Configured thread pool with {executor._max_workers} workers")

    yield
    # Code to run on shutdown (e.g., close database connections)
    logger.info("Application shutting down.")


# Create FastAPI app with the new lifespan manager
app = FastAPI(
    title="Financial Data API",
    description="High-performance financial analysis API with async support",
    version="2.0.0",
    lifespan=lifespan, docs_url=None, redoc_url=None
)

# Create FastAPI app with async optimizations
origins = [
    "https://quanvest.vercel.app",  # Your Vercel frontend URL
    "http://localhost:3000",        # Your local frontend dev server
    "http://127.0.0.1:8000",        # For direct local testing if needed
    "https://api.quanvest.me" ,
    "https://www.quanvest.me"# If your frontend also makes calls to itself, or if you access docs via domain
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"], # Make sure to allow 'X-API-Key' if you use specific headers
)

# Add CORS middleware


logger.debug("CORS middleware configured")


# CRITICAL: Configure async settings for better concurrency



# --- Custom Docs URLs with API Key Protection ---
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(api_key: str = Depends(get_api_key_docs)):
    return get_swagger_ui_html(openapi_url=app.openapi_url, title=app.title)

@app.get("/redoc", include_in_schema=False)
async def custom_redoc_html(api_key: str = Depends(get_api_key_docs)):
    return get_redoc_html(openapi_url=app.openapi_url, title=app.title)

@app.get("/openapi.json", include_in_schema=False)
async def get_openapi_json(api_key: str = Depends(get_api_key_docs)):
    return app.openapi()

# --- Example Protected Endpoint ---
@app.get("/protected_data")
async def get_protected_data(api_key: str = Depends(get_api_key)):
    """
    This endpoint requires a valid API key in the 'X-API-Key' header.
    """
    return {"message": "This is sensitive data, only accessible with a valid API key!"}

# --- Example Unprotected Endpoint (if you have any) ---
@app.get("/public_info")
async def get_public_info():
    """
    This endpoint is publicly accessible without an API key.
    """
    return {"message": "This is public information."}


# Include routers, optionally applying API key protection
# Example: app.include_router(financials.router, prefix="/financials", dependencies=[Depends(get_api_key)])
logger.debug("Registering routers")
app.include_router(home.router, prefix='/home', dependencies=[Depends(get_api_key)])
app.include_router(financials.router, prefix="/financials", dependencies=[Depends(get_api_key)])
app.include_router(ratios.router, prefix="/ratios", dependencies=[Depends(get_api_key)])
app.include_router(stock_data.router, prefix="/stock_data", dependencies=[Depends(get_api_key)])
app.include_router(overview.router, prefix="/overview", dependencies=[Depends(get_api_key)])
app.include_router(charts.router, prefix="/charts", dependencies=[Depends(get_api_key)])
app.include_router(sql_rag.router, dependencies=[Depends(get_api_key)])  # This includes /rag_flask endpoints
app.include_router(search.router, dependencies=[Depends(get_api_key)])
app.include_router(dividend.router, prefix="/dividend", dependencies=[Depends(get_api_key)])
app.include_router(shareholding_pattern.router, prefix="/shareholding_pattern", dependencies=[Depends(get_api_key)])
app.include_router(annual_files.router, prefix="/annual_files", dependencies=[Depends(get_api_key)])
app.include_router(quarterly_files.router, prefix="/quarterly_files", dependencies=[Depends(get_api_key)])
app.include_router(earning_calls.router, prefix="/earning_calls", dependencies=[Depends(get_api_key)])
app.include_router(insider_trading.router, prefix="/insider_trading", dependencies=[Depends(get_api_key)])
app.include_router(pledged_data.router, prefix="/pledged_data", dependencies=[Depends(get_api_key)])
# New Corporate Governance routers
app.include_router(cg_board_composition.router, prefix="/cg_board_composition", dependencies=[Depends(get_api_key)])
app.include_router(cg_committee_composition.router, prefix="/cg_committee_composition", dependencies=[Depends(get_api_key)])
app.include_router(cg_board_meetings.router, prefix="/cg_board_meetings", dependencies=[Depends(get_api_key)])
# New RPT and CG Committee Meetings routers
app.include_router(rpt.router, prefix="/rpt", dependencies=[Depends(get_api_key)])
app.include_router(cg_committee_meetings.router, prefix="/cg_committee_meetings", dependencies=[Depends(get_api_key)])

logger.info("All routers registered")


@app.get("/")
async def root():
    logger.debug("Root endpoint called")
    return {"message": "Welcome to the Financial Data API with Async Support"}


# Add middleware for request timing
@app.middleware("http")
async def add_process_time_header(request, call_next):
    import time
    start_time = time.time()

    response = await call_next(request)

    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)

    # Log slow requests
    if process_time > 5.0:
        logger.warning(f"Slow request: {request.method} {request.url.path} took {process_time:.2f}s")

    return response


if __name__ == "__main__":
    import uvicorn

    logger.info("Starting application server with async optimizations")

    # Run with optimal async settings
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1,  # Single worker for development
        loop="asyncio",  # Use asyncio event loop
        access_log=True
    )