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
    nstime = nstime.split(".")
    if len(nstime) == 1:
        return nstime[0]
    else:
        return ".".join(nstime[:-1])


def dtime2str(dtime):
    if is_timezone_aware(dtime):
        return datetime.strptime(dtime, "%Y-%m-%dT%H:%M:%S.%f%z")

    else:
        return datetime.strptime(dtime, "%Y-%m-%dT%H:%M:%S")


# class DatastoreConnection:
# def __init__(self, ds_host: str, ds_port: str) -> None:
#     self._channel = grpc.insecure_channel(ds_host + ":" + ds_port)
#     self._stub = dstore_grpc.DatastoreStub(self._channel)

#     self.is_channel_ready()

#     logger.info(f"Established connection to {ds_host} gRPC service")

# def is_channel_ready(self):
#     try:
#         grpc.channel_ready_future(self._channel).result(timeout=10)
#     except grpc.FutureTimeoutError:
#         logger.exception(
#             grpc.FutureTimeoutError(
#                 "Connection to the grpc service timed out, " "and was not available at application" "start."
#             )
#         )
#         raise grpc.FutureTimeoutError(
#             "Connection to the grpc service timed out, " "and was not available at application start."
#         )
#     except Exception as e:
#         logger.exception(f"Unhandled exepction {e}")
#         raise


def ingest(msg: str) -> None:
    ts_metadata = dstore.TSMetadata()
    # json_format.ParseDict(msg, ts_metadata, ignore_unknown_fields=True,
    #   max_recursion_depth=100)
    # json_format.ParseDict(msg["properties"], ts_metadata, ignore_unknown_fields=True)
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

    observation_data = dstore.ObsMetadata(
        pubtime=dtime2tstamp(datetime.strptime(msg["properties"]["pubtime"], "%Y-%m-%dT%H:%M:%S.%f%z")),
        obstime_instant=dtime2tstamp(
            datetime.strptime(nstime2stime(msg["properties"]["datetime"]), "%Y-%m-%dT%H:%M:%S")
        ),
        geo_point=dstore.Point(
            lat=float(msg["geometry"]["coordinates"]["lat"]), lon=float(msg["geometry"]["coordinates"]["lon"])
        ),
    )

    for i in ["id", "history", "metadata_id", "processing_level", "data_id", "value"]:
        if i in msg:
            setattr(observation_data, i, msg[i])
        elif i in msg["properties"]:
            setattr(observation_data, i, msg["properties"][i])
        elif i in msg["properties"]["content"]:
            setattr(observation_data, i, msg["properties"]["content"][i])

    # json_format.ParseDict(msg, observation_data,
    #                       ignore_unknown_fields=True, max_recursion_depth=100)
    # json_format.ParseDict(msg["properties"], observation_data, ignore_unknown_fields=True)

    # json_format.ParseDict(msg["properties"]["content"],
    #                       observation_data, ignore_unknown_fields=True)

    # observation_data["obstime"] = Timestamp().FromDatetime(
    #     datetime.strptime(msg["properties"]["datetime"],
    #                       "%Y-%m-%dT%H:%M:%S"))

    request = dstore.PutObsRequest(observations=[dstore.Metadata1(ts_mdata=ts_metadata, obs_mdata=observation_data)])

    try:
        putObsRequest(request)
        # self._stub.PutObservations(request)
    except grpc.RpcError as e:
        logger.critical(str(e))
        pass


def ingest_list(self, msg_list: list) -> None:
    for i in msg_list:
        self.ingest(i)


def putObsRequest(put_obs_request):
    try:
        channel = grpc.aio.insecure_channel(
            f"{os.getenv('DATASTORE_HOST', 'localhost')}:{os.getenv('DATASTORE_PORT', '50050')}"
        )
        grpc_stub = dstore_grpc.DatastoreStub(channel)
    except grpc._channel._InactiveRpcError as e:
        print("Failed to connect:", e)
    else:
        print("Connection established successfully.")

    try:
        grpc_stub.PutObservations(put_obs_request)
        print("RPC call succeeded.")
    except grpc._channel._InactiveRpcError as e:
        print("RPC call failed:", e)