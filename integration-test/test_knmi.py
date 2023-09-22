# Note that this assumes that the KNMI test data is loader (using loader container)
import os
from datetime import datetime

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp


NUMBER_OF_PARAMETERS = 44


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

    assert len(response.tseries) == NUMBER_OF_PARAMETERS


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
        None,
        ["06260", "06348", "06356"],
    ),
    (
        # Multiple stations with a single parameter
        ((52.15, 4.90), (52.15, 5.37), (51.66, 5.37), (51.66, 4.90)),
        ["rh"],
        ["06260", "06348", "06356"],
    ),
    (
        # Multiple stations with multiple parameters
        ((52.15, 4.90), (52.15, 5.37), (51.66, 5.37), (51.66, 4.90)),
        ["dd", "rh", "tx"],
        ["06260", "06348", "06356"],
    ),
    (
        # One station within
        ((52.11, 5.15), (52.11, 5.204), (52.08, 5.204), (52.08, 5.15)),
        None,
        ["06260"],
    ),
    (
        # Nothing within
        ((51.82, 5.07), (51.82, 5.41), (51.73, 5.41), (51.73, 5.07)),
        None,
        [],
    ),
    (
        # Middle top
        ((52.0989, 4.17), (52.0989, 6.18), (52.09, 6.18), (52.09, 4.17)),
        None,
        ["06260"],
    ),
    (
        # Middle bottom, should fall outside since polygon is curved because the earth is round (postgres geography).
        ((52.1, 4.17), (52.1, 6.18), (52.0989, 6.18), (52.0989, 4.17)),
        None,
        [],
    ),
    (
        # Complex polygon
        ((51.45, 3.47), (51.39, 3.67), (51.39, 4.28), (51.52, 4.96), (51.89, 5.46), (52.18, 5.30), (51.75, 3.68)),
        None,
        ["06260", "06310", "06323", "06340", "06343", "06348", "06350", "06356"],
    ),
    (
        # All stations in the Netherlands
        ((56.00, 2.85), (56.00, 7.22), (50.75, 7.22), (50.75, 2.85)),
        None,
        # fmt: off
        [
            "06201", "06203", "06204", "06205", "06207", "06208", "06211", "06214", "06215", "06225", "06229",
            "06235", "06239", "06240", "06242", "06248", "06249", "06251", "06252", "06257", "06258", "06260",
            "06267", "06269", "06270", "06273", "06275", "06277", "06278", "06279", "06280", "06283", "06286",
            "06290", "06310", "06317", "06319", "06320", "06321", "06323", "06330", "06340", "06343", "06344",
            "06348", "06350", "06356", "06370", "06375", "06377", "06380", "06391"
        ],
        # fmt: on
    ),
]


@pytest.mark.parametrize("coords,param_ids,expected_station_ids", input_params_polygon)
def test_get_observations_with_polygon(grpc_stub, coords, param_ids, expected_station_ids):
    ts_request = dstore.FindTSRequest(
        inside=dstore.Polygon(points=[dstore.Point(lat=lat, lon=lon) for lat, lon in coords]), param_ids=param_ids
    )
    ts_response = grpc_stub.FindTimeSeries(ts_request)

    actual_station_ids = sorted({ts.metadata.station_id for ts in ts_response.tseries})
    assert actual_station_ids == expected_station_ids
    number_of_parameters = len(param_ids) if param_ids else NUMBER_OF_PARAMETERS
    expected_number_of_timeseries = number_of_parameters * len(expected_station_ids)
    assert len(ts_response.tseries) == expected_number_of_timeseries
