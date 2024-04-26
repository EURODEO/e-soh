import logging
import os

from functools import cache

import datastore_pb2_grpc as dstore_grpc
import grpc


logger = logging.getLogger(__name__)


@cache
def get_grpc_stub():
    channel = grpc.aio.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}")
    return dstore_grpc.DatastoreStub(channel)


async def putObsRequest(put_obs_request):
    grpc_stub = get_grpc_stub()
    try:
        response = await grpc_stub.PutObservations(put_obs_request)
        logger.info("RPC call succeeded.")
    except grpc._channel._InactiveRpcError as e:
        logger.critical("RPC call failed:", e, response)
        raise e
    except Exception as e:
        raise e
