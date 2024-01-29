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
from pydantic import BaseModel
from routers import edr  # , records

logger = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(BrotliMiddleware)


class HealthCheck(BaseModel):
    """
    Response model for health check
    """

    status: str = "OK"

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
# app.include(records.router)


@app.get("/health", tags=["healthcheck"])
def get_health() -> HealthCheck:
    """
    Small health check to post a response if API is allive.
    This should probably be expanded to also include availablity of grpc backend service.
    """

    return HealthCheck(status="OK")
