import logging
import uuid
import json
from datetime import datetime
from datetime import timezone

import xarray as xr
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


def build_message(file: object, input_type: str, uuid_prefix: str, schema_path: str, validator: object):
    match input_type:
        case "netCDF":
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
        json_msg["properties"]["timeseries_id"] = message_uuid
        json_msg["properties"]["data_id"] = message_uuid
        json_msg["properties"]["pubtime"] = datetime.now(timezone.utc).isoformat()

    return unfinished_messages  # now populated with timestamps and uuids


def load_files(file: str, input_type: str, uuid_prefix: str):
    match input_type:
        case "netCDF":
            ds = xr.load_dataset(file)
            return build_message(ds, input_type, uuid_prefix)
        case "bufr":
            return build_message(file, input_type, uuid_prefix)


def messages(message, input_type, uuid_prefix, schema_path, validator):
    if input_type == "bufr":
        return build_message(
            message, input_type=input_type, uuid_prefix=uuid_prefix, schema_path=schema_path, validator=validator
        )
    elif input_type == "json":
        return build_message(
            message, input_type=input_type, uuid_prefix=uuid_prefix, schema_path=schema_path, validator=validator
        )
    if isinstance(message, str):
        return load_files(message, input_type=input_type, uuid_prefix=uuid_prefix)
    elif isinstance(message, xr.Dataset):
        return build_message(
            message, input_type=input_type, uuid_prefix=uuid_prefix, schema_path=schema_path, validator=validator
        )
    else:
        logger.error(TypeError("Unknown netCDF type, expected path " + f"or xarray.Dataset, got {type(message)}"))
        raise HTTPException(
            status_code=400, detail=f"Unknown netCDF type, expected path or xarray.Dataset, got {type(message)}"
        )
