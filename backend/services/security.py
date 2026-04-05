# backend/security.py
import os
from fastapi import  Request, Query
from fastapi import Header, HTTPException, status
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)
logger.info(f"Loaded API_ACCESS_KEY:{os.environ.get('API_ACCESS_KEY')}")
# IMPORTANT: Store your actual API key securely, e.g., in an environment variable.
# For now, you can hardcode it for testing, but CHANGE THIS FOR PRODUCTION.
# It's better to load from .env or AWS Secrets Manager.
API_KEY = os.environ.get("API_ACCESS_KEY", "YOUR_GENERATED_API_KEY_HERE") # Replace with your actual key

async def get_api_key_docs(
    request: Request,
    x_api_key: str = Header(None),
    api_key: str = Query(None)
):
    # Allow query param only for docs endpoints
    if request.url.path in ["/docs", "/redoc", "/openapi.json"]:
        key = x_api_key or api_key
    else:
        key = x_api_key

    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")
    return key

async def get_api_key(x_api_key: str = Header(..., alias="X-API-Key")):
    """
    FastAPI dependency to validate the API key from the 'X-API-Key' header.
    """
    if x_api_key == API_KEY:
        return x_api_key
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API Key",
    )