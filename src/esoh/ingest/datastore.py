import esoh.datastore_pb2 as dstore
import esoh.datastore_pb2_grpc as dstore_grpc

import grpc
import logging

from google.protobuf import json_format


logger = logging.getLogger(__name__)

class datastore_connection():
    def __init__(self, DSHOST, DSPORT) -> None:
        self._channel = grpc.insecure_channel(f"{DSHOST}:{DSPORT}")
        self._stub = dstore_grpc.Datastorestub(self._channel)

    def ingest(self, msg: str) -> None:
        ts_metadata = dstore.TSMetadata()
        json_format.Parse(msg, ts_metadata, ignore_unknown_fields=True)

        Observation_data = dstore.ObsMetadata()
        json_format.Parse(msg, Observation_data, ignore_unknown_fields=True)

        request = dstore.PutObsRequest(
            observations=[
                dstore.Metadta1(
                    ts_mdata=ts_metadata,
                    obs_mdata=Observation_data
                )
            ]
        )

        try:
            response = self._stub.PutObservations(request)
        except grpc.RpcError as e:


        response
