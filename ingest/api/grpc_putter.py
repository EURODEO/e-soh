import logging
import os
import json

from functools import cache

import datastore_pb2_grpc as dstore_grpc
import grpc
from fastapi import HTTPException

logger = logging.getLogger(__name__)


@cache
def get_grpc_stub():
    grpc_config = json.dumps(
        {
            "methodConfig": {
                "retryPolicy": {
                    "maxAttempts": 10,
                    "initialBackoff": "0.1s",
                    "maxBackoff": "10s",
                    "backoffMultiplier": 2,
                    "retryAbleStatusCodes": ["INTERNAL"],
                }
            }
        }
    )
    channel = grpc.aio.insecure_channel(
        f"{os.getenv('DSHOST', 'store')}:{os.getenv('DSPORT', '50050')}", options=[("grpc.service_config", grpc_config)]
    )

    return dstore_grpc.DatastoreStub(channel)


async def putObsRequest(put_obs_request):
    grpc_stub = get_grpc_stub()
    try:
        await grpc_stub.PutObservations(put_obs_request)
        logger.debug("RPC call succeeded.")
    except grpc.aio.AioRpcError as grpc_error:
        logger.critical(f"RPC call failed: {grpc_error.code()}\n{grpc_error.details()}")
        raise HTTPException(detail=grpc_error.details(), status_code=400)
