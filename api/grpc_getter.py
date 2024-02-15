import os

import datastore_pb2_grpc as dstore_grpc
import grpc

# Functions in this file should be made async,
# These functions should be the only components that are
# dependent on external services.


async def getObsRequest(get_obs_request):
    channel = grpc.aio.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}")
    grpc_stub = dstore_grpc.DatastoreStub(channel)
    response = await grpc_stub.GetObservations(get_obs_request)

    return response


async def getTSAGRequest(get_tsag_request):
    channel = grpc.aio.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}")
    grpc_stub = dstore_grpc.DatastoreStub(channel)
    response = await grpc_stub.GetTSAttrGroups(get_tsag_request)

    return response
