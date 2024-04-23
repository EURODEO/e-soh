import io
import logging
import os

import xarray as xr
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import UploadFile
from pydantic import BaseModel

from api.ingest import IngestToPipeline
from api.model import JsonMessageSchema


logger = logging.getLogger(__name__)


class Response(BaseModel):
    status_message: str
    status_code: int


app = FastAPI()

# Define configuration parameters
mqtt_configuration = {
    "host": os.getenv("MQTT_HOST"),
    "topic": os.getenv("MQTT_TOPIC"),
    "username": os.getenv("MQTT_USERNAME"),
    "password": os.getenv("MQTT_PASSWORD"),
}


@app.post("/nc")
async def upload_netcdf_file(files: UploadFile):
    try:
        ingester = IngestToPipeline(mqtt_conf=mqtt_configuration, uuid_prefix="uuid")
        contents = await files.read()
        ds = xr.open_dataset(io.BytesIO(contents))
        ingester.ingest(ds, "nc")

    except HTTPException as httpexp:
        raise httpexp
    except Exception as e:
        logger.critical(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    return Response(status_message="Successfully ingested", status_code=200)


@app.post("/bufr")
async def upload_bufr_file(files: UploadFile):
    try:
        ingester = IngestToPipeline(mqtt_conf=mqtt_configuration, uuid_prefix="uuid")
        contents = await files.read()
        # filename = files.filename
        ingester.ingest(contents, "bufr")

    except HTTPException as httpexp:
        raise httpexp
    except Exception as e:
        logger.critical(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    return Response(status_message="Successfully ingested", status_code=200)


@app.post("/json")
async def post_json(request: JsonMessageSchema) -> Response:
    try:
        ingester = IngestToPipeline(mqtt_conf=mqtt_configuration, uuid_prefix="uuid")
        ingester.ingest(request.model_dump(exclude_none=True), "json")

    except HTTPException as httpexp:
        raise httpexp
    except Exception as e:
        logger.critical(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    return Response(status_message="Successfully ingested", status_code=200)
