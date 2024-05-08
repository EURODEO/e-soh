import logging
import uuid
import hashlib
from datetime import datetime
from datetime import timezone

from fastapi import HTTPException
from api.model import JsonMessageSchema
from pydantic import ValidationError


from ingest.bufr.create_mqtt_message_from_bufr import (
    build_all_json_payloads_from_bufr,
)

logger = logging.getLogger(__name__)


def build_json_payload(bufr: object):

    unfinished_messages = build_all_json_payloads_from_bufr(bufr)
    if len(unfinished_messages) >= 1:
        for json_msg in unfinished_messages:
            try:
                json_msg["properties"]["platform"] = 1
                JsonMessageSchema(**json_msg)
                logger.debug("Message passed schema validation.")
            except ValidationError as v_error:
                logger.error(" Message did not pass schema validation, " + "\n" + str(v_error))
                json_msg = None
                raise HTTPException(status_code=400, detail=str(v_error))

    else:
        logger.error("Empty message")
        raise HTTPException(status_code=400, detail="Empty message")
    return unfinished_messages


def build_messages(message: object, uuid_prefix: str):

    # Set message publication time in RFC3339 format
    # Create UUID for the message, and state message format version
    for json_msg in message:
        message_uuid = f"{uuid_prefix}:{str(uuid.uuid4())}"
        json_msg["id"] = message_uuid
        json_msg["properties"]["data_id"] = message_uuid

        #  MD5 hash of a join on naming_authority, platform, standard_name, level,function and period.
        timeseries_id_string = (
            json_msg["properties"]["naming_authority"]
            + json_msg["properties"]["platform"]
            + json_msg["properties"]["content"]["standard_name"]
            + json_msg["properties"]["level"]
            + json_msg["properties"]["function"]
            + json_msg["properties"]["period"]
        )
        timeseries_id = hashlib.md5(timeseries_id_string.encode()).hexdigest()
        json_msg["properties"]["timeseries_id"] = timeseries_id
        json_msg["properties"]["pubtime"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    return message  # now populated with timestamps and uuids
