import esoh.datastore_pb2 as dstore
import esoh.datastore_pb2_grpc as dstore_grpc

import grpc
import logging

from google.protobuf.timestamp_pb2 import Timestamp

from datetime import datetime

import re

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
        return datetime.strptime(
            dtime, "%Y-%m-%dT%H:%M:%S.%f%z")

    else:
        return datetime.strptime(
            dtime, "%Y-%m-%dT%H:%M:%S")


class datastore_connection():
    def __init__(self, DSHOST: str, DSPORT: str) -> None:
        self._channel = grpc.insecure_channel(DSHOST + ":" + DSPORT)
        self._stub = dstore_grpc.DatastoreStub(self._channel)

        self.is_channel_ready()

        logger.info(f"Established connection to {DSHOST} gRPC service")

    def is_channel_ready(self):
        try:
            grpc.channel_ready_future(self._channel).result(timeout=10)
        except grpc.FutureTimeoutError:
            logger.exception(grpc.FutureTimeoutError("Connection to the grpc service timed out, "
                                                     "and was not available at application"
                                                     "start."))
            raise grpc.FutureTimeoutError("Connection to the grpc service timed out, "
                                          "and was not available at application start.")
        except Exception as e:
            logger.exception(f"Unhandled exepction {e}")
            raise

    def ingest(self, msg: str) -> None:
        ts_metadata = dstore.TSMetadata()
        # json_format.ParseDict(msg, ts_metadata, ignore_unknown_fields=True,
        #   max_recursion_depth=100)
        # json_format.ParseDict(msg["properties"], ts_metadata, ignore_unknown_fields=True)
        for i in ['version',
                  'type',
                  'title',
                  'summary',
                  'keywords',
                  'keywords_vocabulary',
                  'license',
                  'conventions',
                  'naming_authority',
                  'creator_type',
                  'creator_name',
                  'creator_email',
                  'creator_url',
                  'institution',
                  'project',
                  'source',
                  'platform',
                  'platform_vocabulary',
                  'standard_name',
                  'unit',
                  'instrument',
                  'instrument_vocabulary']:
            if i in msg["properties"]:
                setattr(ts_metadata, i, msg["properties"][i])
            elif i in msg["properties"]["content"]:
                setattr(ts_metadata, i, msg["properties"]["content"][i])

        Observation_data = dstore.ObsMetadata(
            pubtime=dtime2tstamp(datetime.strptime(
                msg["properties"]["pubtime"], "%Y-%m-%dT%H:%M:%S.%f%z")),
            obstime_instant=dtime2tstamp(
                datetime.strptime(nstime2stime(msg["properties"]["datetime"]),
                                  "%Y-%m-%dT%H:%M:%S")),
            geo_point=dstore.Point(lat=float(msg["geometry"]["coordinates"][0]),
                                   lon=float(msg["geometry"]["coordinates"][1]))
        )

        for i in ['id',
                  'history',
                  'metadata_id',
                  'processing_level',
                  'data_id',
                  'value']:
            if i in msg:
                setattr(Observation_data, i, msg[i])
            elif i in msg['properties']:
                setattr(Observation_data, i, msg["properties"][i])
            elif i in msg["properties"]["content"]:
                setattr(Observation_data, i, msg["properties"]["content"][i])

        # json_format.ParseDict(msg, Observation_data,
        #                       ignore_unknown_fields=True, max_recursion_depth=100)
        # json_format.ParseDict(msg["properties"], Observation_data, ignore_unknown_fields=True)
        # json_format.ParseDict(msg["properties"]["content"],
        #                       Observation_data, ignore_unknown_fields=True)

        # Observation_data["obstime"] = Timestamp().FromDatetime(
        #     datetime.strptime(msg["properties"]["datetime"],
        #                       "%Y-%m-%dT%H:%M:%S"))

        request = dstore.PutObsRequest(
            observations=[
                dstore.Metadata1(
                    ts_mdata=ts_metadata,
                    obs_mdata=Observation_data
                )
            ]
        )

        try:
            self._stub.PutObservations(request)
        except grpc.RpcError as e:
            logger.critical(str(e))
            pass
            raise

    def ingest_list(self, msg_list: list) -> None:
        for i in msg_list:
            self.ingest(i)
