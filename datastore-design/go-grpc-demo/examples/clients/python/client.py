#!/usr/bin/env python3

# tested with Python 3.9

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc


def callAddTSRequest(stub):
    print('calling AddTSRequest() ...')
    tsMData = dstore.TSMetadata(
        field1='value1',
        field2='value2',
        field3='value3',
    )
    request = dstore.AddTSRequest(
        id=1234,
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
    obs = [
        dstore.Observation(
            time=10,
            value=123.456,
            metadata=obsMData,
        )
    ]
    request = dstore.PutObsRequest(
        tsobs=[
            dstore.TSObservations(
                tsid=1234,
                obs=obs,
            )
        ],
    )
    response = stub.PutObservations(request)
    print('    response: {}'.format(response))


def callGetObservations(stub):
    print('calling GetObservations() ...')
    request = dstore.GetObsRequest(
        tsids=[1234, 5678, 9012],
        fromtime=156,
        totime=163,
    )
    response = stub.GetObservations(request)
    print('    response: {}'.format(response))


if __name__ == '__main__':

    with grpc.insecure_channel('localhost:50050') as channel:
        stub = dstore_grpc.DatastoreStub(channel)

        callAddTSRequest(stub)
        callPutObservations(stub)
        callGetObservations(stub)
