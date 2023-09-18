# Note that this assumes that the KNMI test data is loader (using loader container)
import logging
import os
from datetime import datetime
from datetime import timezone

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _handle_grpc_error(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except grpc.RpcError as rpc_error:
        for error in rpc_error.args:
            if error.details.endswith("already exists"):
                logger.info(error)
            else:
                logger.error(rpc_error)
                raise rpc_error


@pytest.fixture(scope="session")
def grpc_stub():
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        yield dstore_grpc.DatastoreStub(channel)


def dummy_timeseries_for_delete(fake_id, fake_str_id):
    ts_metadata = dstore.TSMetadata(
        station_id=fake_str_id,
        param_id=fake_str_id,
        pos=dstore.Point(lat=-60, lon=-160),
        other1="test_value1",
        other2="test_value2",
        other3="test_value3",
    )
    ts_add_request = dstore.AddTSRequest(
        id=fake_id,
        metadata=ts_metadata,
    )
    return ts_add_request


def dummy_observations_for_delete(fake_id):
    obs_metadata = dstore.ObsMetadata(field1="test_value1", field2="test_value2")
    time_1 = Timestamp()
    time_1.FromDatetime(datetime(year=1999, month=9, day=9, hour=9, minute=9, tzinfo=timezone.utc))
    time_2 = Timestamp()
    time_2.FromDatetime(datetime(year=1999, month=9, day=9, hour=9, minute=10, tzinfo=timezone.utc))
    time_3 = Timestamp()
    time_3.FromDatetime(datetime(year=1999, month=9, day=9, hour=9, minute=11, tzinfo=timezone.utc))
    obs = [
        dstore.Observation(time=time_1, value=1111.1111, metadata=obs_metadata),
        dstore.Observation(time=time_2, value=2222.2222, metadata=obs_metadata),
        dstore.Observation(time=time_3, value=3333.3333, metadata=obs_metadata),
    ]
    obs_put_request = dstore.PutObsRequest(
        tsobs=[dstore.TSObservations(tsid=fake_id, obs=obs)],
    )
    return obs_put_request


def test_delete_timeseries(grpc_stub):
    fake_id = 999999999
    fake_str_id = "999999999"
    ts_add_request = dummy_timeseries_for_delete(fake_id=fake_id, fake_str_id=fake_str_id)
    obs_put_request = dummy_observations_for_delete(fake_id=fake_id)

    ts_add_response = grpc_stub.AddTimeSeries(ts_add_request)
    assert str(ts_add_response) == "status: -1\n"

    obs_response = grpc_stub.PutObservations(obs_put_request)
    assert str(obs_response) == "status: -1\n"

    ts_find_request = dstore.FindTSRequest(station_ids=[fake_str_id], param_ids=[fake_str_id])
    ts_find_response = grpc_stub.FindTimeSeries(ts_find_request)
    assert len(ts_find_response.tseries) == 1
    assert ts_find_response.tseries[0].id == fake_id

    to_time = Timestamp()
    to_time.FromDatetime(datetime(year=1999, month=9, day=9, hour=9, minute=11, second=1, tzinfo=timezone.utc))
    obs_get_request = dstore.GetObsRequest(
        tsids=[fake_id], fromtime=obs_put_request.tsobs[0].obs[0].time, totime=to_time
    )
    obs_get_response = grpc_stub.GetObservations(obs_get_request)
    assert obs_get_response.tsobs[0].tsid == fake_id
    assert len(obs_get_response.tsobs[0].obs) == 3

    ts_delete_request = dstore.DeleteTSRequest(ids=[fake_id])
    ts_delete_response = grpc_stub.DeleteTimeSeries(ts_delete_request)
    assert str(ts_delete_response) == "status: -1\n"

    ts_find_response = grpc_stub.FindTimeSeries(ts_find_request)
    assert len(ts_find_response.tseries) == 0

    obs_get_response = grpc_stub.GetObservations(obs_get_request)
    assert obs_get_response.tsobs[0].tsid == fake_id
    assert len(obs_get_response.tsobs[0].obs) == 0


@pytest.fixture(scope="module")
def setup_fake_data_for_polygon(grpc_stub):
    fake_id = 999999990
    fake_str_id = "999999990"
    ts_metadata = dstore.TSMetadata(
        station_id=fake_str_id,
        param_id=fake_str_id,
        pos=dstore.Point(lat=80, lon=170),
        other1="test_value1",
        other2="test_value2",
        other3="test_value3",
    )
    ts_add_request_1 = dstore.AddTSRequest(
        id=fake_id,
        metadata=ts_metadata,
    )
    # grpc_stub.AddTimeSeries(ts_add_request_1)
    _handle_grpc_error(grpc_stub.AddTimeSeries, ts_add_request_1)

    fake_id = 999999991
    fake_str_id = "999999991"
    ts_metadata = dstore.TSMetadata(
        station_id=fake_str_id,
        param_id=fake_str_id,
        pos=dstore.Point(lat=90, lon=180),
        other1="test_value4",
        other2="test_value5",
        other3="test_value6",
    )
    ts_add_request_2 = dstore.AddTSRequest(
        id=fake_id,
        metadata=ts_metadata,
    )
    # grpc_stub.AddTimeSeries(ts_add_request_2)
    _handle_grpc_error(grpc_stub.AddTimeSeries, ts_add_request_2)

    yield  # Hereafter teardown

    grpc_stub.DeleteTimeSeries(dstore.DeleteTSRequest(ids=[ts_add_request_1.id, ts_add_request_2.id]))


input_params_polygon = [
    (
        # Multiple stations within
        ((80, 170), (90, 170), (90, 180), (80, 180)),
        {"number_of_stations": 2, "station_ids": [999999990, 999999991]},
    ),
    (
        # One station within
        ((80, 175), (90, 175), (90, 180), (80, 180)),
        {"number_of_stations": 1, "station_ids": [999999991]},
    ),
    (
        # Nothing within
        ((80, 175), (85, 175), (85, 180), (80, 180)),
        {"number_of_stations": 0, "station_ids": []},
    ),
    (
        # middle top
        ((85, 120), (90, 120), (90, -120), (85, -120)),
        {"number_of_stations": 1, "station_ids": [999999991]},
    ),
    (
        # Middle bottom, should fall outside since polygon is curved because the earth is round (postgres geography).
        ((85, 120), (80, 120), (85, -120), (80, -120)),
        {"number_of_stations": 0, "station_ids": []},
    ),
    (
        # Complex polygon
        ((79, 169), (81, 169), (80, 171), (89, 171), (89, 180), (75, 180), (75, 171)),
        {"number_of_stations": 1, "station_ids": [999999990]},
    ),
]


@pytest.mark.parametrize("coords,expected", input_params_polygon)
def test_get_observations_with_polygon(grpc_stub, setup_fake_data_for_polygon, coords, expected):
    ts_request = dstore.FindTSRequest(
        inside=dstore.Polygon(points=[dstore.Point(lat=lat, lon=lon) for lat, lon in coords])
    )
    ts_response = grpc_stub.FindTimeSeries(ts_request)

    assert len(ts_response.tseries) == expected["number_of_stations"]
    assert sorted(ts.id for ts in ts_response.tseries) == expected["station_ids"]
