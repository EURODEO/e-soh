#!/usr/bin/env python3
# tested with Python 3.11
# Generate protobuf code with following command from top level directory:
# python -m grpc_tools.protoc --proto_path=datastore/protobuf datastore.proto --python_out=examples/clients/python --grpc_python_out=examples/clients/python
import os
from datetime import datetime

from google.protobuf.timestamp_pb2 import Timestamp

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc

MAGIC_ID = 1234567890
MAGIC_VALUE = 123.456

def callAddTimeSeries(stub):
    print('calling AddTimeSeries() ...')
    tsMData = dstore.TSMetadata(
        station_id='18700',
        param_id='211',
        pos=dstore.Point(
            lat=59.91,
            lon=10.75,
        ),
        other1='value1',
        other2='value2',
        other3='value3',
    )
    request = dstore.AddTSRequest(
        id=MAGIC_ID,
        metadata=tsMData,
    )
    response = stub.AddTimeSeries(request)
    print('    response: {}'.format(response))


def callPutObservations(stub):
    print('calling PutObservations() ...')
    obsMData = dstore.ObsMetadata(
        field1='value1',
        field2='value2',
    )
    timestamp = Timestamp()
    timestamp.FromDatetime(datetime.now())
    obs = [
        dstore.Observation(
            time=timestamp,
            value=MAGIC_VALUE,
            metadata=obsMData,
        )
    ]
    request = dstore.PutObsRequest(
        tsobs=[
            dstore.TSObservations(
                tsid=MAGIC_ID,
                obs=obs,
            )
        ],
    )
    response = stub.PutObservations(request)
    print('    response: {}'.format(response))


def callGetObservations(stub):
    print('calling GetObservations() ...')
    from_time = Timestamp()
    from_time.FromDatetime(datetime(2023, 1, 1))
    to_time = Timestamp()
    to_time.FromDatetime(datetime(2023, 10, 1))

    request = dstore.GetObsRequest(
        tsids=[1234567890, 5678, 9012],
        fromtime=from_time,
        totime=to_time,
    )
    response = stub.GetObservations(request)
    print('    response: {}'.format(response))

    return response


if __name__ == '__main__':

    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        stub = dstore_grpc.DatastoreStub(channel)

        callAddTimeSeries(stub)
        callPutObservations(stub)
        response = callGetObservations(stub)

    # Check response
    found_at_least_one = False
    for r in response.tsobs:
        if r.tsid == MAGIC_ID:
            for o in r.obs:
                assert(o.value == MAGIC_VALUE)
                found_at_least_one = True
    assert found_at_least_one
