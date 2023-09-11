# Note that this assumes that the KNMI test data is loader (using loader container)
import os
from datetime import datetime


import pytest

from google.protobuf.timestamp_pb2 import Timestamp

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc


@pytest.fixture
def grpc_stub():
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        yield dstore_grpc.DatastoreStub(channel)


def test_find_series_single_station_single_parameter(grpc_stub):
    request = dstore.FindTSRequest(
            station_ids=["06260"],
            param_ids=["rh"]
        )
    response = grpc_stub.FindTimeSeries(request)

    assert len(response.tseries) == 1
    assert response.tseries[0].metadata.pos.lat == 52.098821802977
    assert response.tseries[0].metadata.pos.lon == 5.1797058644882


def test_find_series_all_stations_single_parameter(grpc_stub):
    request = dstore.FindTSRequest(
        param_ids=["rh"]
    )
    response = grpc_stub.FindTimeSeries(request)

    assert len(response.tseries) == 55


def test_find_series_single_station_all_parameters(grpc_stub):
    request = dstore.FindTSRequest(
        station_ids=["06260"],
    )
    response = grpc_stub.FindTimeSeries(request)

    assert len(response.tseries) == 44


def test_get_values_single_station_single_paramters(grpc_stub):
    ts_request = dstore.FindTSRequest(
        station_ids=["06260"],
        param_ids=["rh"]
    )
    ts_response = grpc_stub.FindTimeSeries(ts_request)
    assert len(ts_response.tseries) == 1
    ts_id = ts_response.tseries[0].id

    from_time = Timestamp()
    from_time.FromDatetime(datetime(2022, 12, 31))
    to_time = Timestamp()
    to_time.FromDatetime(datetime(2023, 11, 1))
    request = dstore.GetObsRequest(
        tsids=[ts_id],
        fromtime=from_time,
        totime=to_time,
    )
    response = grpc_stub.GetObservations(request)

    assert len(response.tsobs) == 1
    assert response.tsobs[0].tsid == ts_id
    assert len(response.tsobs[0].obs) == 144
    assert response.tsobs[0].obs[0].value == 95.0
    assert response.tsobs[0].obs[-1].value == 59.0
