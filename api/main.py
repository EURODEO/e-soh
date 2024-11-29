# Run with:
# For developing:    uvicorn main:app --reload
import logging
import os

import metadata_endpoints
from brotli_asgi import BrotliMiddleware
from edr_pydantic.capabilities import ConformanceModel
from edr_pydantic.capabilities import LandingPageModel
from edr_pydantic.collections import Collection
from edr_pydantic.collections import Collections
from fastapi import FastAPI
from fastapi import Request
from routers import edr
from routers import feature
from utilities import create_url_from_request


def setup_logging():
    logger = logging.getLogger()
    syslog = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s ; e-soh-api ; %(process)s ; %(levelname)s ; %(name)s ; %(message)s")

    syslog.setFormatter(formatter)
    logger.addHandler(syslog)


setup_logging()

logger = logging.getLogger(__name__)

app = FastAPI(
    swagger_ui_parameters={"tryItOutEnabled": True},
    root_path=os.getenv("FASTAPI_ROOT_PATH", ""),
)
app.add_middleware(BrotliMiddleware)


@app.get(
    "/",
    tags=["Capabilities"],
    response_model=LandingPageModel,
    response_model_exclude_none=True,
)
async def landing_page(request: Request) -> LandingPageModel:
    return metadata_endpoints.get_landing_page(request)


@app.get("/health", include_in_schema=False)
def health():
    return "ok"


@app.get(
    "/conformance",
    tags=["Capabilities"],
    response_model=ConformanceModel,
    response_model_exclude_none=True,
)
async def get_conformance() -> ConformanceModel:
    return metadata_endpoints.get_conformance()


@app.get(
    "/collections",
    tags=["Capabilities"],
    response_model=Collections,
    response_model_exclude_none=True,
)
async def get_collections(request: Request) -> Collections:
    base_url = create_url_from_request(request)
    return await metadata_endpoints.get_collections(base_url)


@app.get(
    "/collections/observations",
    tags=["Collection metadata"],
    response_model=Collection,
    response_model_exclude_none=True,
)
async def get_collection_metadata(request: Request) -> Collection:
    base_url = create_url_from_request(request)
    return await metadata_endpoints.get_collection_metadata(base_url, is_self=True)


# Include all routes
app.include_router(edr.router)
app.include_router(feature.router)
