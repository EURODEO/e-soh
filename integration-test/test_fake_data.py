# Note that this assumes that the KNMI test data is loader (using loader container)
import os
from datetime import datetime
from datetime import timezone

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp


@pytest.fixture
def grpc_stub():
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        yield dstore_grpc.DatastoreStub(channel)


def test_delete_timeseries(grpc_stub):
    test_value = 999999999
    test_value_str = str(test_value)
    ts_metadata = dstore.TSMetadata(
        station_id=test_value_str,
        param_id=test_value_str,
        pos=dstore.Point(lat=9999.9999, lon=9999.9999),
        other1="test_value1",
        other2="test_value2",
        other3="test_value3",
    )
    ts_add_request = dstore.AddTSRequest(
        id=test_value,
        metadata=ts_metadata,
    )
    ts_add_response = grpc_stub.AddTimeSeries(ts_add_request)
    assert str(ts_add_response) == "status: -1\n"

    obs_metadata = dstore.ObsMetadata(field1="test_value1", field2="test_value2")
    time_1 = Timestamp()
    time_1.FromDatetime(datetime(year=1999, month=9, day=9, hour=9, minute=9, tzinfo=timezone.utc))
    time_2 = Timestamp()
    time_2.FromDatetime(datetime(year=1999, month=9, day=9, hour=9, minute=10, tzinfo=timezone.utc))
    time_3 = Timestamp()
    time_3.FromDatetime(datetime(year=1999, month=9, day=9, hour=9, minute=11, tzinfo=timezone.utc))
    obs = [
        dstore.Observation(time=time_1, value=test_value, metadata=obs_metadata),
        dstore.Observation(time=time_2, value=test_value, metadata=obs_metadata),
        dstore.Observation(time=time_3, value=test_value, metadata=obs_metadata),
    ]
    obs_put_request = dstore.PutObsRequest(
        tsobs=[dstore.TSObservations(tsid=test_value, obs=obs)],
    )
    obs_response = grpc_stub.PutObservations(obs_put_request)
    assert str(obs_response) == "status: -1\n"

    ts_find_request = dstore.FindTSRequest(station_ids=[test_value_str], param_ids=[test_value_str])
    ts_find_response = grpc_stub.FindTimeSeries(ts_find_request)
    assert len(ts_find_response.tseries) == 1
    assert ts_find_response.tseries[0].id == test_value

    to_time = Timestamp()
    to_time.FromDatetime(datetime(year=1999, month=9, day=9, hour=9, minute=11, second=1))
    obs_get_request = dstore.GetObsRequest(tsids=[test_value], fromtime=time_1, totime=to_time)
    obs_get_response = grpc_stub.GetObservations(obs_get_request)
    assert obs_get_response.tsobs[0].tsid == test_value
    assert len(obs_get_response.tsobs[0].obs) == 3

    ts_delete_request = dstore.DeleteTSRequest(ids=[test_value])
    ts_delete_response = grpc_stub.DeleteTimeSeries(ts_delete_request)
    assert str(ts_delete_response) == "status: -1\n"

    ts_find_response = grpc_stub.FindTimeSeries(ts_find_request)
    assert len(ts_find_response.tseries) == 0

    obs_get_response = grpc_stub.GetObservations(obs_get_request)
    assert obs_get_response.tsobs[0].tsid == test_value
    assert len(obs_get_response.tsobs[0].obs) == 0
