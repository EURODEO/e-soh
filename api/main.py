# Run with:
# For developing:    uvicorn main:app --reload
import logging

import metadata_endpoints
from brotli_asgi import BrotliMiddleware
from edr_pydantic.capabilities import LandingPageModel
from edr_pydantic.collections import Collection
from edr_pydantic.collections import Collections
from fastapi import FastAPI
from fastapi import Request
from routers import edr
from routers import records

logger = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(BrotliMiddleware)


@app.get(
    "/",
    tags=["Capabilities"],
    response_model=LandingPageModel,
    response_model_exclude_none=True,
)
async def landing_page(request: Request) -> LandingPageModel:
    return metadata_endpoints.get_landing_page(request)


@app.get(
    "/collections",
    tags=["Capabilities"],
    response_model=Collections,
    response_model_exclude_none=True,
)
async def get_collections(request: Request) -> Collections:
    return metadata_endpoints.get_collections(request)


@app.get(
    "/collections/observations",
    tags=["Collection metadata"],
    response_model=Collection,
    response_model_exclude_none=True,
)
async def get_collection_metadata(request: Request) -> Collection:
    return metadata_endpoints.get_collection_metadata(request)


# Include all routes
app.include_router(edr.router)
app.include_router(records.router)
