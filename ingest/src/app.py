import io

import xarray as xr
from esoh.ingest.main import IngestToPipeline
from fastapi import FastAPI
from fastapi import UploadFile
from model import JsonMessageSchema
from pydantic import BaseModel


class Item(BaseModel):
    name: str
    id: int
    description: str | None = None


app = FastAPI()

# Define configuration parameters
mqtt_configuration = {
    "broker_url": "mqtt://your_mqtt_broker",
    "username": "your_username",
    "password": "your_password",
    # ... other MQTT configuration options
}

datastore_configuration = {
    "host": "your_datastore_host",
    "port": 1234,
    "username": "your_datastore_username",
    "password": "your_datastore_password",
    # ... other datastore configuration options
}


@app.post("/uploadfile/")
async def create_upload_file(files: UploadFile, input_type: str):
    ingester = IngestToPipeline(
        mqtt_conf=mqtt_configuration, dstore_conn=datastore_configuration, uuid_prefix="uuid", testing=True
    )
    contents = await files.read()
    ds = xr.open_dataset(io.BytesIO(contents))
    response = ingester.ingest(ds, input_type)
    return response


@app.post("/json")
async def post_json(request: JsonMessageSchema, input_type: str):
    ingester = IngestToPipeline(
        mqtt_conf=mqtt_configuration, dstore_conn=datastore_configuration, uuid_prefix="uuid", testing=True
    )
    response = ingester.ingest(request.dict(exclude_none=True), input_type)
    return response
