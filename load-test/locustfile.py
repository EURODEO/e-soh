# Use the following command to generate the python protobuf stuff in the correct place (from the root of the repository)
# python -m grpc_tools.protoc --proto_path=datastore/protobuf datastore.proto --python_out=load-test --grpc_python_out=load-test

import random
from datetime import datetime

# import grpc_user
import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
from locust import task

from google.protobuf.timestamp_pb2 import Timestamp

import time
from typing import Any, Callable
import grpc
import grpc.experimental.gevent as grpc_gevent
from grpc_interceptor import ClientInterceptor
from locust import User
from locust.exception import LocustError

# patch grpc so that it uses gevent instead of asyncio
grpc_gevent.init_gevent()


class LocustInterceptor(ClientInterceptor):
    def __init__(self, environment, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.env = environment

    def intercept(
            self,
            method: Callable,
            request_or_iterator: Any,
            call_details: grpc.ClientCallDetails,
    ):
        response = None
        exception = None
        start_perf_counter = time.perf_counter()
        response_length = 0
        try:
            response = method(request_or_iterator, call_details)
            response_length = response.result().ByteSize()
        except grpc.RpcError as e:
            exception = e

        self.env.events.request.fire(
            request_type="grpc",
            name=call_details.method,
            response_time=(time.perf_counter() - start_perf_counter) * 1000,
            response_length=response_length,
            response=response,
            context=None,
            exception=exception,
        )
        return response


class GrpcUser(User):
    abstract = True
    stub_class = None

    def __init__(self, environment):
        super().__init__(environment)
        for attr_value, attr_name in ((self.host, "host"), (self.stub_class, "stub_class")):
            if attr_value is None:
                raise LocustError(f"You must specify the {attr_name}.")

        self._channel = grpc.insecure_channel(self.host)
        interceptor = LocustInterceptor(environment=environment)
        self._channel = grpc.intercept_channel(self._channel, interceptor)

        self.stub = self.stub_class(self._channel)


class StoreGrpcUser(GrpcUser):
    host = "localhost:50050"
    stub_class = dstore_grpc.DatastoreStub

    @task
    def find_debilt_humidity(self):
        ts_request = dstore.FindTSRequest(
            station_ids=["06260"],
            param_ids=["rh"]
        )
        ts_response = self.stub.FindTimeSeries(ts_request)
        assert len(ts_response.tseries) == 1

    @task
    def get_data_random_timeserie(self):
        ts_id = random.randint(1, 55*44)

        from_time = Timestamp()
        from_time.FromDatetime(datetime(2022, 12, 31))
        to_time = Timestamp()
        to_time.FromDatetime(datetime(2023, 11, 1))
        request = dstore.GetObsRequest(
            tsids=[ts_id],
            fromtime=from_time,
            totime=to_time,
        )
        response = self.stub.GetObservations(request)
        assert len(response.tsobs[0].obs) == 144

