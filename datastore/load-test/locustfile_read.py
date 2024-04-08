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
    "0-20000-0-06203", "0-20000-0-06204", "0-20000-0-06205", "0-20000-0-06207", "0-20000-0-06208", "0-20000-0-06211",
    "0-20000-0-06214", "0-20000-0-06215", "0-20000-0-06235", "0-20000-0-06239", "0-20000-0-06242", "0-20000-0-06251",
    "0-20000-0-06260", "0-20000-0-06269", "0-20000-0-06270", "0-20000-0-06275", "0-20000-0-06279", "0-20000-0-06280",
    "0-20000-0-06290", "0-20000-0-06310", "0-20000-0-06317", "0-20000-0-06319", "0-20000-0-06323", "0-20000-0-06330",
    "0-20000-0-06340", "0-20000-0-06344", "0-20000-0-06348", "0-20000-0-06350", "0-20000-0-06356", "0-20000-0-06370",
    "0-20000-0-06375", "0-20000-0-06380", "0-20000-0-78871", "0-20000-0-78873",
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

    @task
    def get_data_for_single_timeserie(self):
        from_time = Timestamp()
        from_time.FromDatetime(datetime(2022, 12, 31))
        to_time = Timestamp()
        to_time.FromDatetime(datetime(2023, 1, 1))

        request = dstore.GetObsRequest(
            temporal_interval=dstore.TimeInterval(start=from_time, end=to_time),
            filter=dict(
                platform=dstore.Strings(values=[random.choice(stations)]),
                instrument=dstore.Strings(values=[random.choice(parameters)]),
            ),
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
            temporal_interval=dstore.TimeInterval(start=from_time, end=to_time),
            filter=dict(instrument=dstore.Strings(values=[random.choice(parameters)])),
            spatial_polygon=dstore.Polygon(
                points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]
            ),
        )
        response = self.stub.GetObservations(request)
        assert len(response.observations) == 1
        assert len(response.observations[0].obs_mdata) == 144
