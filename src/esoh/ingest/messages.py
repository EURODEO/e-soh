from datetime import datetime, timezone

import xarray as xr

import uuid
import logging

from esoh.ingest.netCDF.extract_metadata_netcdf import build_all_json_payloads_from_netCDF

from jsonschema import ValidationError

logger = logging.getLogger(__name__)


def build_message(file: [object],
                  input_type: str,
                  uuid_prefix: str,
                  schema_path: str,
                  validator: object):
    match input_type:
        case "netCDF":
            unfinished_messages = build_all_json_payloads_from_netCDF(file,
                                                                      schema_path=schema_path)

    # Set message publication time in RFC3339 format
    # Create UUID for the message, and state message format version
    for json_msg in unfinished_messages:

        message_uuid = f"{uuid_prefix}:{str(uuid.uuid4())}"
        json_msg["id"] = message_uuid
        json_msg["properties"]["metadata_id"] = message_uuid
        json_msg["properties"]["data_id"] = message_uuid

        json_msg["properties"]["pubtime"] = datetime.now(timezone.utc).isoformat()
        try:
            validator.validate(json_msg)
        except ValidationError as v_error:
            logger.error("Message did not pass schema validation, " + "\n" + str(v_error.message))
            json_msg = None
            raise

    return unfinished_messages  # now populated with timestamps and uuids


def load_files(file: str, input_type: str, uuid_prefix: str):
    match input_type:
        case "netCDF":
            ds = xr.load_dataset(file)
            return build_message(ds, input_type, uuid_prefix)


def messages(message, input_type, uuid_prefix, schema_path, validator):
    if isinstance(message, str):
        return load_files(message, input_type=input_type, uuid_prefix=uuid_prefix)
    elif isinstance(message, xr.Dataset):
        return build_message(message,
                             input_type=input_type,
                             uuid_prefix=uuid_prefix,
                             schema_path=schema_path,
                             validator=validator)
    else:
        raise TypeError("Unknown netCDF type, expected path "
                        + f"or xarray.Dataset, got {type(message)}")
