import os

import datastore_pb2_grpc as dstore_grpc
import grpc

# Functions in this file should be made async,
# These functions should be the only components that are
# dependent on external services.


class gRPCRequest:
    """
    Class for keeping a gRPC channel and stub alive for a workers entire
    lifespan.
    """

    def __init__(self):
        self.channel = grpc.aio.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}")
        self.grpc_stub = dstore_grpc.DatastoreStub(self.channel)

    async def getObsRequest(self, get_obs_request):
        response = await self.grpc_stub.GetObservations(get_obs_request)
        return response

    async def getTSAGRequest(self, get_tsag_request):
        response = await self.grpc_stub.GetTSAttrGroups(get_tsag_request)
        return response
