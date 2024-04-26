import logging
import os
import re

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import UploadFile
from pydantic import BaseModel

from typing import List
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
    "username": os.getenv("MQTT_USERNAME"),
    "password": os.getenv("MQTT_PASSWORD"),
}


# @app.post("/nc")
# async def upload_netcdf_file(files: UploadFile):
#     try:
#         ingester = IngestToPipeline(mqtt_conf=mqtt_configuration, uuid_prefix="uuid")
#         contents = await files.read()
#         ds = xr.open_dataset(io.BytesIO(contents))
#         ingester.ingest(ds, "nc")

#     except HTTPException as httpexp:
#         raise httpexp
#     except Exception as e:
#         logger.critical(e)
#         raise HTTPException(status_code=500, detail="Internal server error")

#     return Response(status_message="Successfully ingested", status_code=200)


@app.post("/bufr")
async def upload_bufr_file(files: UploadFile):
    try:
        ingester = IngestToPipeline(mqtt_conf=mqtt_configuration, uuid_prefix="uuid")
        input_type = _decide_input_type(files.filename)
        contents = await files.read()
        ingester.ingest(contents, input_type)

    except HTTPException as httpexp:
        raise httpexp
    except Exception as e:
        logger.critical(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    return Response(status_message="Successfully ingested", status_code=200)


@app.post("/json")
async def post_json(request: JsonMessageSchema | List[JsonMessageSchema]) -> Response:
    try:
        ingester = IngestToPipeline(mqtt_conf=mqtt_configuration, uuid_prefix="uuid")
        await ingester.ingest(request.model_dump(exclude_none=True), "json")

    except HTTPException as httpexp:
        raise httpexp
    except Exception as e:
        logger.critical(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    return Response(status_message="Successfully ingested", status_code=200)


def _decide_input_type(message) -> str:
    """
    Internal method for deciding what type of input is being provided.
    """
    file_name = os.path.basename(message)
    if re.match(r"data[0-9][0-9][0-9][05]$", file_name):
        return "bufr"
    match message.split(".")[-1].lower():
        case "bufr" | "buf":
            return "bufr"
        case _:
            logger.critical(f"Unknown filetype provided. Got {message.split('.')[-1]}")
            raise HTTPException(status_code=400, detail=f"Unknown filetype provided. Got {message.split('.')[-1]}")
