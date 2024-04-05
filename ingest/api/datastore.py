import logging
import os
import re
from datetime import datetime

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
from google.protobuf.timestamp_pb2 import Timestamp

logger = logging.getLogger(__name__)


def dtime2tstamp(dtime):
    tstamp = Timestamp()
    tstamp.FromDatetime(dtime)
    return tstamp


def is_timezone_aware(dt):
    if re.search(r"\+\d{2}:\d{2}$", dt) or re.search(r"\[z,Z]$", dt):
        return True
    else:
        return False


def nstime2stime(nstime):
    nstime = nstime.split("+")
    nstime = nstime[0].split(".")
    if len(nstime) == 1:
        return nstime[0]
    else:
        return ".".join(nstime[:-1])


def dtime2str(dtime):
    if is_timezone_aware(dtime):
        return datetime.strptime(dtime, "%Y-%m-%dT%H:%M:%S.%f%z")

    else:
        return datetime.strptime(dtime, "%Y-%m-%dT%H:%M:%S")


def ingest(msg: str) -> None:
    ts_metadata = dstore.TSMetadata()
    for i in [
        "version",
        "type",
        "title",
        "summary",
        "keywords",
        "keywords_vocabulary",
        "license",
        "conventions",
        "naming_authority",
        "creator_type",
        "creator_name",
        "creator_email",
        "creator_url",
        "institution",
        "project",
        "source",
        "platform",
        "platform_vocabulary",
        "standard_name",
        "unit",
        "instrument",
        "instrument_vocabulary",
        "level",
        "period",
        "function",
    ]:
        if i in msg["properties"]:
            setattr(ts_metadata, i, msg["properties"][i])
        elif i in msg["properties"]["content"]:
            setattr(ts_metadata, i, msg["properties"]["content"][i])
    level = str(ts_metadata.level)

    period = ts_metadata.period
    function = ts_metadata.function
    standard_name = ts_metadata.standard_name
    parameter_name = ":".join([standard_name, level, period, function])
    setattr(ts_metadata, "parameter_name", parameter_name)

    # TODO: rename in e-soh-message-spec.json instead of doing the below translation
    # NOTE: in that case "timeseries_id" needs to be added to the list iterated over above
    #
    # Special case for metadata_id -> timeseries_id. This translation is required for naming
    # consistency with the terminology used internally in the datastore where a 'time series'
    # represents metadata that usually don't change with obs time. This metadata is represented
    # in the TSMetadata message in datastore.proto and used for defining request- and response
    # messages for several methods.
    from_name, to_name = "metadata_id", "timeseries_id"
    if from_name in msg["properties"]:
        setattr(ts_metadata, to_name, msg["properties"][from_name])
    elif from_name in msg["properties"]["content"]:
        setattr(ts_metadata, to_name, msg["properties"]["content"][from_name])

    observation_data = dstore.ObsMetadata(
        pubtime=dtime2tstamp(datetime.strptime(msg["properties"]["pubtime"], "%Y-%m-%dT%H:%M:%S.%f%z")),
        obstime_instant=dtime2tstamp(
            datetime.strptime(nstime2stime(msg["properties"]["datetime"]), "%Y-%m-%dT%H:%M:%S")
        ),
        geo_point=dstore.Point(
            lat=float(msg["geometry"]["coordinates"]["lat"]), lon=float(msg["geometry"]["coordinates"]["lon"])
        ),
    )

    for i in ["id", "history", "processing_level", "data_id", "value"]:
        if i in msg:
            setattr(observation_data, i, msg[i])
        elif i in msg["properties"]:
            setattr(observation_data, i, msg["properties"][i])
        elif i in msg["properties"]["content"]:
            setattr(observation_data, i, msg["properties"]["content"][i])

    request = dstore.PutObsRequest(observations=[dstore.Metadata1(ts_mdata=ts_metadata, obs_mdata=observation_data)])

    try:
        putObsRequest(request)
    except grpc.RpcError as e:
        logger.critical(str(e))


def putObsRequest(put_obs_request):
    try:
        channel = grpc.aio.insecure_channel(
            f"{os.getenv('DATASTORE_HOST', 'localhost')}:{os.getenv('DATASTORE_PORT', '50050')}"
        )
        grpc_stub = dstore_grpc.DatastoreStub(channel)
    except grpc._channel._InactiveRpcError as e:
        logger.error("Failed to connect to datastore:", e)

    else:
        logger.info("Connection to datastore established successfully.")

    try:
        grpc_stub.PutObservations(put_obs_request)
        logger.info("RPC call succeeded.")
    except grpc._channel._InactiveRpcError as e:
        logger.critical("RPC call failed:", e)
