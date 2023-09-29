from datetime import datetime

import xarray as xr

import uuid

from esoh.ingest.netCDF.extract_metadata_netcdf import build_all_json_payloads_from_netCDF


def build_message(file: [object], input_type: str, uuid_prefix: str):
    match input_type:
        case "netCDF":
            unfinnished_messages = build_all_json_payloads_from_netCDF(file)

            # Set message publication time in RFC3339 format
            # Create UUID for the message, and state message format version
            for json_msg in unfinnished_messages:

                message_uuid = f"{uuid_prefix}:{str(uuid.uuid4())}"
                json_msg["id"] = message_uuid
                json_msg["properties"]["metadata_id"] = message_uuid
                json_msg["properties"]["data_id"] = message_uuid
                current_time = datetime.utcnow().replace(microsecond=0)
                current_time_str = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')

                json_msg["properties"][
                    "pubtime"] = f"{current_time_str[:-3]}{current_time_str[-3:].zfill(6)}Z"

            return unfinnished_messages  # now populated with timestamps and uuids


def load_files(file: str, input_type: str, uuid_prefix: str):
    match input_type:
        case "netCDF":
            ds = xr.load_dataset(file)
            return build_message(ds, input_type, uuid_prefix)
