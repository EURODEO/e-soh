import io
import logging
import os

import xarray as xr
from fastapi import FastAPI
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
    "host": os.getenv("MQTT_HOST", "localhost"),
    "topic": os.getenv("MQTT_TOPIC", "esoh"),
    "username": os.getenv("MQTT_USERNAME", "username"),
    "password": os.getenv("MQTT_PASSWORD", "password"),
}

# datastore_configuration = {
#     "dshost": os.getenv("DATASTORE_HOST", "localhost"),
#     "dsport": os.getenv("DATASTORE_PORT", "1234"),
#     "username": os.getenv("DATASTORE_USERNAME", "username"),
#     "password": os.getenv("DATASTORE_PASSWORD", "password"),
# }


@app.post("/nc")
async def upload_netcdf_file(files: UploadFile, input_type: str = "nc"):
    try:
        ingester = IngestToPipeline(mqtt_conf=mqtt_configuration, uuid_prefix="uuid", testing=True)
        contents = await files.read()
        ds = xr.open_dataset(io.BytesIO(contents))
        response, status = ingester.ingest(ds, input_type)
        return Response(status_message=response, status_code=status)

    except Exception as e:
        # No specfic exceptions are thrown from ingest
        # So catch all and send a generic response back
        return Response(status_message=str(e), status_code=500)


@app.post("/bufr")
async def upload_bufr_file(files: UploadFile, input_type: str = "bufr"):
    try:
        ingester = IngestToPipeline(mqtt_conf=mqtt_configuration, uuid_prefix="uuid", testing=True)
        contents = await files.read()
        response, status = ingester.ingest(contents, input_type)
        return Response(status_message=response, status_code=status)

    except Exception as e:
        # No specfic exceptions are thrown from ingest
        # So catch all and send a generic response back
        return Response(status_message=str(e), status_code=500)


@app.post("/json")
async def post_json(request: JsonMessageSchema, input_type: str):
    try:
        ingester = IngestToPipeline(mqtt_conf=mqtt_configuration, uuid_prefix="uuid", testing=True)
        response, status = ingester.ingest(request.dict(exclude_none=True), input_type)
        return Response(status_message=response, status_code=status)

    except Exception as e:
        return Response(status_message=str(e), status_code=500)
