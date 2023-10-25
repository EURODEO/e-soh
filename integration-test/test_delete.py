# import logging
# import os
# from datetime import datetime
# from datetime import timezone
#
# import datastore_pb2 as dstore
# import datastore_pb2_grpc as dstore_grpc
# import grpc
# import pytest
# from google.protobuf.timestamp_pb2 import Timestamp
#
#
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
#
#
# @pytest.fixture(scope="session")
# def grpc_stub():
#     with grpc.insecure_channel(
#         f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}"
#     ) as channel:
#         yield dstore_grpc.DatastoreStub(channel)
#
#
# @pytest.fixture(scope="function")
# def dummy_timeseries_for_delete():
#     dummy_id = 999999999
#     dummy_str_id = "test_delete-1"
#
#     ts_metadata = dstore.TSMetadata(
#         station_id=dummy_str_id,
#         param_id=dummy_str_id,
#         pos=dstore.Point(lat=-60, lon=-160),
#         other1="test_value1",
#         other2="test_value2",
#         other3="test_value3",
#     )
#     ts_add_request = dstore.AddTSRequest(
#         id=dummy_id,
#         metadata=ts_metadata,
#     )
#     return ts_add_request
#
#
# @pytest.fixture(scope="function")
# def dummy_observations_for_delete():
#     dummy_id = 999999999
#
#     obs_metadata = dstore.ObsMetadata(field1="test_value1", field2="test_value2")
#     time_1 = Timestamp()
#     time_1.FromDatetime(
#         datetime(year=1999, month=9, day=9, hour=9, minute=9, tzinfo=timezone.utc)
#     )
#     time_2 = Timestamp()
#     time_2.FromDatetime(
#         datetime(year=1999, month=9, day=9, hour=9, minute=10, tzinfo=timezone.utc)
#     )
#     time_3 = Timestamp()
#     time_3.FromDatetime(
#         datetime(year=1999, month=9, day=9, hour=9, minute=11, tzinfo=timezone.utc)
#     )
#     obs = [
#         dstore.Observation(time=time_1, value=1111.1111, metadata=obs_metadata),
#         dstore.Observation(time=time_2, value=2222.2222, metadata=obs_metadata),
#         dstore.Observation(time=time_3, value=3333.3333, metadata=obs_metadata),
#     ]
#     obs_put_request = dstore.PutObsRequest(
#         tsobs=[dstore.TSObservations(tsid=dummy_id, obs=obs)],
#     )
#     return obs_put_request
#
#
# def test_delete_timeseries(
#         grpc_stub, dummy_timeseries_for_delete, dummy_observations_for_delete
# ):
#     grpc_stub.AddTimeSeries(dummy_timeseries_for_delete)
#
#     grpc_stub.PutObservations(dummy_observations_for_delete)
#
#     ts_find_request = dstore.FindTSRequest(
#         station_ids=[dummy_timeseries_for_delete.metadata.station_id],
#         param_ids=[dummy_timeseries_for_delete.metadata.param_id],
#     )
#     ts_find_response = grpc_stub.FindTimeSeries(ts_find_request)
#     assert len(ts_find_response.tseries) == 1
#     assert ts_find_response.tseries[0].id == dummy_timeseries_for_delete.id
#
#     to_time = Timestamp()
#     to_time.FromDatetime(
#         datetime(year=1999, month=9, day=9, hour=9, minute=11, second=1, tzinfo=timezone.utc)
#     )
#     obs_get_request = dstore.GetObsRequest(
#         tsids=[dummy_timeseries_for_delete.id],
#         fromtime=dummy_observations_for_delete.tsobs[0].obs[0].time,
#         totime=to_time,
#     )
#     obs_get_response = grpc_stub.GetObservations(obs_get_request)
#     assert obs_get_response.tsobs[0].tsid == dummy_timeseries_for_delete.id
#     assert len(obs_get_response.tsobs[0].obs) == 3
#
#     ts_delete_request = dstore.DeleteTSRequest(ids=[dummy_timeseries_for_delete.id])
#     grpc_stub.DeleteTimeSeries(ts_delete_request)
#
#     ts_find_response = grpc_stub.FindTimeSeries(ts_find_request)
#     assert len(ts_find_response.tseries) == 0
#
#     obs_get_response = grpc_stub.GetObservations(obs_get_request)
#     assert obs_get_response.tsobs[0].tsid == dummy_timeseries_for_delete.id
#     assert len(obs_get_response.tsobs[0].obs) == 0
