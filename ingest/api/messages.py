import logging
import uuid
import json
from datetime import datetime
from datetime import timezone

from fastapi import HTTPException
from jsonschema import ValidationError
from api.model import JsonMessageSchema


from ingest.bufr.create_mqtt_message_from_bufr import (
    build_all_json_payloads_from_bufr,
)
from ingest.netCDF.extract_metadata_netcdf import (
    build_all_json_payloads_from_netcdf,
)

logger = logging.getLogger(__name__)


def build_messages(file: object, input_type: str, uuid_prefix: str, schema_path: str):
    match input_type:
        case "nc":
            unfinished_messages = build_all_json_payloads_from_netcdf(file, schema_path=schema_path)
        case "bufr":
            unfinished_messages = build_all_json_payloads_from_bufr(file)
        case "json":
            unfinished_messages = []
            unfinished_messages.append(file)

    # Set message publication time in RFC3339 format
    # Create UUID for the message, and state message format version
    for json_msg in unfinished_messages:
        try:
            JsonMessageSchema.model_validate_json(json.dumps(json_msg))
            logger.info("Message passed schema validation.")
        except ValidationError as v_error:
            logger.error("Message did not pass schema validation, " + "\n" + str(v_error.message))
            json_msg = None
            raise HTTPException(status_code=400, detail="Message did not pass schema validation")
        message_uuid = f"{uuid_prefix}:{str(uuid.uuid4())}"
        json_msg["id"] = message_uuid
        json_msg["properties"]["metadata_id"] = message_uuid
        json_msg["properties"]["data_id"] = message_uuid
        json_msg["properties"]["pubtime"] = datetime.now(timezone.utc).isoformat()

    return unfinished_messages  # now populated with timestamps and uuids
