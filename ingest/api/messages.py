import logging
import uuid
import hashlib
from datetime import datetime
from datetime import timezone

from api.model import JsonMessageSchema


from bufr_tools.create_mqtt_message_from_bufr import (
    build_all_json_payloads_from_bufr,
)

logger = logging.getLogger(__name__)

msg_alias = {"function": "method", "period": "duration", "period_int": ""}


def message_properties_to_public(message: object):
    for key in msg_alias:
        if key in message["properties"]:
            if len(msg_alias[key]):
                message["properties"][msg_alias[key]] = message["properties"][key]
            del message["properties"][key]
    return message


def build_json_payload(bufr: object):

    unfinished_messages = build_all_json_payloads_from_bufr(bufr)
    loaded_schemas = [JsonMessageSchema(**i) for i in unfinished_messages]
    return loaded_schemas


def build_messages(message: object, uuid_prefix: str):

    # Set message publication time in RFC3339 format
    # Create UUID for the message, and state message format version
    for json_msg in message:
        period = json_msg["properties"]["period_int"]
        message_uuid = f"{uuid_prefix}:{str(uuid.uuid4())}"
        json_msg["id"] = message_uuid
        json_msg["properties"]["data_id"] = message_uuid
        json_msg["properties"]["period"] = period
        #  MD5 hash of a join on naming_authority, platform, standard_name, level,function and period.
        timeseries_id_string = (
            json_msg["properties"]["naming_authority"]
            + json_msg["properties"]["platform"]
            + json_msg["properties"]["content"]["standard_name"]
            + str(json_msg["properties"]["level"])
            + json_msg["properties"]["function"]
            + str(json_msg["properties"]["period"])
        )
        timeseries_id = hashlib.md5(timeseries_id_string.encode()).hexdigest()
        json_msg["properties"]["timeseries_id"] = timeseries_id
        json_msg["properties"]["pubtime"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    return message  # now populated with timestamps and uuids
