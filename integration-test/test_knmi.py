# Note that this assumes that the KNMI test data is loader (using loader container)
import os
from datetime import datetime

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp


@pytest.fixture(scope="session")
def grpc_stub():
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        yield dstore_grpc.DatastoreStub(channel)


def test_find_series_single_station_single_parameter(grpc_stub):
    request = dstore.FindTSRequest(station_ids=["06260"], param_ids=["rh"])
    response = grpc_stub.FindTimeSeries(request)

    assert len(response.tseries) == 1
    assert response.tseries[0].metadata.pos.lat == 52.098821802977
    assert response.tseries[0].metadata.pos.lon == 5.1797058644882


def test_find_series_all_stations_single_parameter(grpc_stub):
    request = dstore.FindTSRequest(param_ids=["rh"])
    response = grpc_stub.FindTimeSeries(request)

    assert len(response.tseries) == 55


def test_find_series_single_station_all_parameters(grpc_stub):
    request = dstore.FindTSRequest(
        station_ids=["06260"],
    )
    response = grpc_stub.FindTimeSeries(request)

    assert len(response.tseries) == 44


def test_get_values_single_station_single_parameters(grpc_stub):
    ts_request = dstore.FindTSRequest(station_ids=["06260"], param_ids=["rh"])
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


input_params_polygon = [
    (
        # Multiple stations within
        ((52.15, 4.90), (52.15, 5.37), (51.66, 5.37), (51.66, 4.90)),
        {"number_of_timeseries": 132, "station_ids": ["06260", "06348", "06356"]},
    ),
    (
        # One station within
        ((52.11, 5.15), (52.11, 5.204), (52.08, 5.204), (52.08, 5.15)),
        {"number_of_timeseries": 44, "station_ids": ["06260"]},
    ),
    (
        # Nothing within
        ((51.82, 5.07), (51.82, 5.41), (51.73, 5.41), (51.73, 5.07)),
        {"number_of_timeseries": 0, "station_ids": []},
    ),
    (
        # Middle top
        ((52.0989, 4.17), (52.0989, 6.18), (52.09, 6.18), (52.09, 4.17)),
        {"number_of_timeseries": 44, "station_ids": ["06260"]},
    ),
    (
        # Middle bottom, should fall outside since polygon is curved because the earth is round (postgres geography).
        ((52.1, 4.17), (52.1, 6.18), (52.0989, 6.18), (52.0989, 4.17)),
        {"number_of_timeseries": 0, "station_ids": []},
    ),
    (
        # Complex polygon
        ((51.45, 3.47), (51.39, 3.67), (51.39, 4.28), (51.52, 4.96), (51.89, 5.46), (52.18, 5.30), (51.75, 3.68)),
        {
            "number_of_timeseries": 352,
            "station_ids": ["06260", "06310", "06323", "06340", "06343", "06348", "06350", "06356"],
        },
    ),
]


@pytest.mark.parametrize("coords,expected", input_params_polygon)
def test_get_observations_with_polygon(grpc_stub, coords, expected):
    ts_request = dstore.FindTSRequest(
        inside=dstore.Polygon(points=[dstore.Point(lat=lat, lon=lon) for lat, lon in coords])
    )
    ts_response = grpc_stub.FindTimeSeries(ts_request)

    assert len(ts_response.tseries) == expected["number_of_timeseries"]  # parameters * number_of_stations
    assert sorted({ts.metadata.station_id for ts in ts_response.tseries}) == expected["station_ids"]
