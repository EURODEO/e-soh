import io
import os
from enum import Enum

import xarray as xr
from esoh.ingest.main import IngestToPipeline
from fastapi import FastAPI
from fastapi import UploadFile
from model import JsonMessageSchema
from pydantic import BaseModel


class Status(Enum):
    ERROR = "error"
    SUCCESS = "success"


class Message(BaseModel):
    status: Status
    detail: str


class Response(BaseModel):
    content: dict
    status_code: int


app = FastAPI()

# Define configuration parameters
mqtt_configuration = {
    "host": os.getenv("MQTT_HOST", "localhost"),
    "topic": os.getenv("MQTT_TOPIC", "esoh"),
    "username": os.getenv("MQTT_USERNAME", "username"),
    "password": os.getenv("MQTT_PASSWORD", "password"),
}

datastore_configuration = {
    "dshost": os.getenv("DATASTORE_HOST", "localhost"),
    "dsport": os.getenv("DATASTORE_PORT", "1234"),
    "username": os.getenv("DATASTORE_USERNAME", "username"),
    "password": os.getenv("DATASTORE_PASSWORD", "password"),
}


@app.post("/uploadfile/")
async def create_upload_file(files: UploadFile, input_type: str):
    ingester = IngestToPipeline(
        mqtt_conf=mqtt_configuration, dstore_conn=datastore_configuration, uuid_prefix="uuid", testing=True
    )
    contents = await files.read()
    ds = xr.open_dataset(io.BytesIO(contents))
    ingester.ingest(ds, input_type)


@app.post("/json")
async def post_json(request: JsonMessageSchema, input_type: str):
    try:
        ingester = IngestToPipeline(
            mqtt_conf=mqtt_configuration, dstore_conn=datastore_configuration, uuid_prefix="uuid", testing=True
        )
        ingester.ingest(request.dict(exclude_none=True), input_type)
        success_message = Message(status=Status.SUCCESS, detail="Operation successful")
        return Response(content=success_message.dict(), status_code=200)

    except Exception as e:
        # No specfic exceptions are thrown from ingest
        # So catch all and send a generic response back
        error_message = Message(status=Status.ERROR, detail=str(e))
        return Response(content=error_message.dict(), status_code=500)
