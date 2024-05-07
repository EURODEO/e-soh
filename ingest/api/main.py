import logging
import os
import sys

from fastapi import FastAPI
from fastapi import UploadFile
from pydantic import BaseModel

from typing import List
from api.ingest import IngestToPipeline
from api.model import JsonMessageSchema
from api.messages import build_json_payload


logger = logging.getLogger(__name__)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


class Response(BaseModel):
    status_message: str
    status_code: int


app = FastAPI()

# Define configuration parameters
mqtt_configuration = {
    "host": os.getenv("MQTT_HOST"),
    "username": os.getenv("MQTT_USERNAME"),
    "password": os.getenv("MQTT_PASSWORD"),
}


ingester = IngestToPipeline(mqtt_conf=mqtt_configuration, uuid_prefix="uuid")


@app.post("/bufr")
async def upload_bufr_file(files: UploadFile):
    contents = await files.read()
    json_data = build_json_payload(contents)
    await ingester.ingest(json_data)

    return Response(status_message="Successfully ingested", status_code=200)


@app.post("/json")
async def post_json(request: JsonMessageSchema | List[JsonMessageSchema]) -> Response:
    status = "Successfully ingested"
    if isinstance(request, list):
        hash_list = [i.__hash__() for i in request]
        unique_request = [request[hash_list.index(i)] for i in set(hash_list)]
        if len(unique_request) != len(request):
            status = "Insert accepted, duplicates removed."

        json_data = [item.model_dump(exclude_none=True) for item in unique_request]
    else:
        json_data = [request.model_dump(exclude_none=True)]

    await ingester.ingest(json_data)

    return Response(status_message=status, status_code=200)
