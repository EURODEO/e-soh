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
            "methodConfig": [
                {
                    "name": [{}],
                    "retryPolicy": {
                        "maxAttempts": 5,
                        "initialBackoff": "0.5s",
                        "maxBackoff": "8s",
                        "backoffMultiplier": 2,
                        "retryableStatusCodes": ["INTERNAL", "UNAVAILABLE"],
                    },
                }
            ]
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
        raise HTTPException(detail=f"GRPC_ERROR:{grpc_error.details()}", status_code=400)
