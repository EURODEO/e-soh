import logging
import os
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


def ingest(msg: str) -> None:
    """
    This method sets up required fields in TSMetadata, ObsMetadata and ingest data to datastore
    """
    ts_metadata = dstore.TSMetadata()
    field_list_ts = ts_metadata.DESCRIPTOR.fields_by_name.keys()

    for i in field_list_ts:
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

    observation_data = dstore.ObsMetadata()
    field_list_obs = observation_data.DESCRIPTOR.fields_by_name.keys()
    for i in field_list_obs:
        if i == "obstime_instant":
            if "datetime" in msg["properties"]:
                obstime_instant = dtime2tstamp(
                    datetime.strptime((msg["properties"]["datetime"]), "%Y-%m-%dT%H:%M:%S%z")
                )
                observation_data.obstime_instant.CopyFrom(obstime_instant)
        elif i in msg:
            setattr(observation_data, i, msg[i])
        elif i in msg["properties"]:
            if i == "pubtime":
                pubtime = dtime2tstamp(datetime.strptime(msg["properties"]["pubtime"], "%Y-%m-%dT%H:%M:%S%z"))
                observation_data.pubtime.CopyFrom(pubtime)
            else:
                setattr(observation_data, i, msg["properties"][i])
        elif i in msg["properties"]["content"]:
            setattr(observation_data, i, msg["properties"]["content"][i])

    if "geometry" in msg:
        if msg["geometry"]["type"] == "Point":
            point = dstore.Point(
                lat=float(msg["geometry"]["coordinates"]["lat"]), lon=float(msg["geometry"]["coordinates"]["lon"])
            )
            observation_data.geo_point.CopyFrom(point)

    request = dstore.PutObsRequest(observations=[dstore.Metadata1(ts_mdata=ts_metadata, obs_mdata=observation_data)])

    try:
        putObsRequest(request)
    except grpc.RpcError as e:
        logger.critical(str(e))
        raise e


def putObsRequest(put_obs_request):
    try:
        channel = grpc.aio.insecure_channel(
            f"{os.getenv('DATASTORE_HOST', 'localhost')}:{os.getenv('DATASTORE_PORT', '50050')}"
        )
        grpc_stub = dstore_grpc.DatastoreStub(channel)
    except grpc._channel._InactiveRpcError as e:
        logger.error("Failed to connect to datastore:", e)
        raise e

    else:
        logger.info("Connection to datastore established successfully.")

    try:
        grpc_stub.PutObservations(put_obs_request)
        logger.info("RPC call succeeded.")
    except grpc._channel._InactiveRpcError as e:
        logger.critical("RPC call failed:", e)
        raise e
