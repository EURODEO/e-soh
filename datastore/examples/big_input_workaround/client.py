"""This client demonstrates how to work around gRPC message size limit by
   calling PutObservations multiple times.
   The overall set of observations is gradually split into smaller and
   smaller parts until each one is successfully accommodated in a single
   request message to PutObservations.

   Tested with Python 3.11

   If necessary, generate protobuf code by running the following command from
   the directory that contains the 'protobuf' subdirectory:

     python -m grpc_tools.protoc --proto_path=protobuf datastore.proto \
         --python_out=../examples/big_input_workaround \
         --grpc_python_out=../examples/big_input_workaround
"""

import argparse
import os
import sys
from collections import deque
from datetime import datetime
from datetime import timezone

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
from google.protobuf.timestamp_pb2 import Timestamp


def dtime2tstamp(dtime):
    tstamp = Timestamp()
    tstamp.FromDatetime(dtime)
    return tstamp


# create_observations creates a set of observations.
def create_observations(obs_count, summary_size):
    # create time series metadata common to all observations
    ts_mdata = dstore.TSMetadata(
        version="dummy_version",
        type="dummy_type",
        summary=("x" * summary_size),
        standard_name="air_temperature",
        unit="celsius",
        # more attributes ...
    )

    pubtime = dtime2tstamp(datetime(2023, 1, 1, 0, 0, 10, 0, tzinfo=timezone.utc))

    obs = []

    # create a set of observations where only the obs time varies
    for i in range(obs_count):
        obs_mdata = dstore.ObsMetadata(
            id="dummy_id",
            geo_point=dstore.Point(
                lat=59.91,
                lon=10.75,
            ),
            pubtime=pubtime,
            data_id="dummy_data_id",
            obstime_instant=Timestamp(seconds=i),
            value="12.7",
            # more attributes ...
        )

        obs.append(
            dstore.Metadata1(
                ts_mdata=ts_mdata,
                obs_mdata=obs_mdata,
            )
        )

    return obs


# call_put_obs demonstrates how to robustly insert observations in the store
# when the set of observations may cause a single request to become too big
# for the gRPC protocol.
# Returns three ints:
#   - total number of observations to be inserted,
#   - number of observations successfully inserted,
#   - total calls to PutObservations.
def call_put_obs(stub, obs_count, summary_size):
    # create overall set of observations to be inserted in the store
    obs = create_observations(obs_count, summary_size)

    stack = deque()

    if len(obs) > 0:
        stack.append(obs)  # push overall set on stack

    tot_inserted = 0  # total observations succesfully inserted
    tot_calls = 0  # total calls to PutObservations

    while len(stack) > 0:  # while more (sub)sets remain
        # try to insert next (sub)set in the store
        obs0 = stack.pop()
        request = dstore.PutObsRequest(observations=obs0)

        try:
            stub.PutObservations(request)
            tot_inserted += len(obs0)
            tot_calls += 1
        except grpc.RpcError as err:
            if err.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                if len(obs0) == 1:  # give up, since even a single observation
                    # (that may not be split further!) is too big for a single message
                    print("error: even a single obs is too big for a single message")
                    break

                # split obs0 into two subsets, push both on stack,
                # and try again
                m = len(obs0) // 2
                obs1, obs2 = obs0[:m], obs0[m:]
                if len(obs1) > 0:
                    stack.append(obs1)
                if len(obs2) > 0:
                    stack.append(obs2)
                continue

            # give up
            print(f"unexpected error (code: {err.code()}; details: {err.details()})")
            break

    # NOTE: at this point, the overall set of observations has been completely
    # inserted in the store only if no errors occurred in the above loop

    return len(obs), tot_inserted, tot_calls


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-n",
        dest="obs_count",
        default=10000,
        type=int,
        metavar="<obs count>",
        help="total number of observations to insert in the data store "
        + "(effectively defines an upper bound of the number of calls to "
        + "PutObservations)",
    )
    parser.add_argument(
        "-s",
        dest="summary_size",
        default=1000,
        type=int,
        metavar="<summary size>",
        help="size of summary attribute (controls the total size of a single message)",
    )
    parse_res = parser.parse_args(sys.argv[1:])
    return parse_res.obs_count, parse_res.summary_size


if __name__ == "__main__":
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        stub = dstore_grpc.DatastoreStub(channel)

        obs_count, summary_size = parse_args()

        tot_obs, tot_ins, tot_calls = call_put_obs(stub, obs_count, summary_size)

        ps = f"{(tot_ins / tot_obs) * 100:.2f}" if tot_obs > 0 else "0.0"

        print(
            f"total observations: {tot_obs}; successfully inserted: "
            + f"{tot_ins} ({ps}%); "
            + f"calls to PutObservations: {tot_calls}"
        )
