import grpc
import os

import datastore_pb2_grpc as dstore_grpc


def get_obsrequest(get_obs_request):
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        grpc_stub = dstore_grpc.DatastoreStub(channel)
        response = grpc_stub.GetObservations(get_obs_request)

    return response
