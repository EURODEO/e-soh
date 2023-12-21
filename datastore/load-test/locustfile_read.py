# Use the following command to generate the python protobuf stuff in
# the correct place (from the root of the repository)
# python -m grpc_tools.protoc --proto_path=datastore/protobuf datastore.proto --python_out=load-test --grpc_python_out=load-test  # noqa: E501
import random
from datetime import datetime

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc_user
from google.protobuf.timestamp_pb2 import Timestamp
from locust import task
from shapely import buffer
from shapely import wkt


parameters = ["ff", "dd", "rh", "pp", "tn"]
# fmt: off
stations = [
    "06203", "06204", "06205", "06207", "06208", "06211", "06214", "06215", "06235", "06239",
    "06242", "06251", "06260", "06269", "06270", "06275", "06279", "06280", "06290", "06310",
    "06317", "06319", "06323", "06330", "06340", "06344", "06348", "06350", "06356", "06370",
    "06375", "06380", "78871", "78873",
]
# fmt: on
points = [
    "POINT(5.179705 52.0988218)",
    "POINT(3.3416666666667 52.36)",
    "POINT(2.9452777777778 53.824130555556)",
    "POINT(4.7811453228565 52.926865008825)",
    "POINT(4.342014 51.447744494043)",
]


class StoreGrpcUser(grpc_user.GrpcUser):
    host = "localhost:50050"
    stub_class = dstore_grpc.DatastoreStub
    weight = 1

    @task
    def get_data_for_single_timeserie(self):
        from_time = Timestamp()
        from_time.FromDatetime(datetime(2022, 12, 31))
        to_time = Timestamp()
        to_time.FromDatetime(datetime(2023, 1, 1))

        request = dstore.GetObsRequest(
            interval=dstore.TimeInterval(start=from_time, end=to_time),
            platforms=[random.choice(stations)],
            instruments=[random.choice(parameters)],
        )
        response = self.stub.GetObservations(request)
        assert len(response.observations) == 1
        assert len(response.observations[0].obs_mdata) == 144

    @task
    def get_data_single_station_through_bbox(self):
        from_time = Timestamp()
        from_time.FromDatetime(datetime(2022, 12, 31))
        to_time = Timestamp()
        to_time.FromDatetime(datetime(2023, 1, 1))

        point = wkt.loads(random.choice(points))
        poly = buffer(point, 0.0001, quad_segs=1)  # Roughly 10 meters around the point

        request = dstore.GetObsRequest(
            interval=dstore.TimeInterval(start=from_time, end=to_time),
            instruments=[random.choice(parameters)],
            inside=dstore.Polygon(points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]),
        )
        response = self.stub.GetObservations(request)
        assert len(response.observations) == 1
        assert len(response.observations[0].obs_mdata) == 144
