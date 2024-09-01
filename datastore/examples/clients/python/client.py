#!/usr/bin/env python3
# tested with Python 3.11
# Generate protobuf code with following command from top level directory:
# python -m grpc_tools.protoc --proto_path=datastore/protobuf datastore.proto \
#   --python_out=examples/clients/python --grpc_python_out=examples/clients/python  # noqa: E501
import os
from datetime import datetime
from datetime import timezone

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
from google.protobuf.timestamp_pb2 import Timestamp


def dtime2tstamp(dtime):
    tstamp = Timestamp()
    tstamp.FromDatetime(dtime)
    return tstamp


# put_obs demonstrates how to insert observations in the datastore.
def put_obs(stub, mdata):

    ts_mdata = dstore.TSMetadata(
        version=mdata["version"],
        type=mdata["type"],
        standard_name=mdata["standard_name"],
        unit=mdata["unit"],
        title=mdata["title"],
        instrument=mdata["instrument"],
        level=mdata["level"],
        function=mdata["function"],
        period=mdata["period"],
        parameter_name=mdata["parameter_name"],
        # add more attributes as required ...
    )

    obs_mdata = dstore.ObsMetadata(
        id="id_dummy",
        geo_point=dstore.Point(
            lat=mdata["lat"],
            lon=mdata["lon"],
        ),
        pubtime=dtime2tstamp(datetime(2023, 1, 1, 0, 0, 10, 0, tzinfo=timezone.utc)),
        data_id="data_id_dummy",
        obstime_instant=dtime2tstamp(datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)),
        value=mdata["value"],
        # add more attributes as required ...
    )

    request = dstore.PutObsRequest(
        observations=[  # insert only a single observation for now
            dstore.Metadata1(
                ts_mdata=ts_mdata,
                obs_mdata=obs_mdata,
            )
        ],
    )

    return stub.PutObservations(request)


# get_obs_in_time_range demonstrates how to retrieve from the datastore all observations in an obs
# time range.
def get_obs_in_time_range(stub):

    request = dstore.GetObsRequest(
        temporal_interval=dstore.TimeInterval(
            start=dtime2tstamp(datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)),
            end=dtime2tstamp(datetime(2023, 1, 2, 0, 0, 0, 0, tzinfo=timezone.utc)),
        )
    )

    return stub.GetObservations(request)


# get_obs_in_polygon demonstrates how to retrieve from the datastore all observations in a polygon.
def get_obs_in_polygon(stub):

    points = []
    points.append(dstore.Point(lat=59.90, lon=10.70))
    points.append(dstore.Point(lat=59.90, lon=10.80))
    points.append(dstore.Point(lat=60, lon=10.80))
    points.append(dstore.Point(lat=60, lon=10.70))

    request = dstore.GetObsRequest(spatial_polygon=dstore.Polygon(points=points))

    return stub.GetObservations(request)


# get_obs_in_circle demonstrates how to retrieve from the datastore all observations in a circle.
def get_obs_in_circle(stub, center, radius):

    request = dstore.GetObsRequest(spatial_circle=dstore.Circle(center=center, radius=radius))

    return stub.GetObservations(request)


# test_put_obs tests that put_obs works as expected.
def test_put_obs(stub, mdata):

    response = put_obs(stub, mdata)
    _ = response  # TODO: add some tests here


# test_get_obs_in_time_range tests that get_obs_in_time_range works as expected.
def test_get_obs_in_time_range(stub, mdata):

    response = get_obs_in_time_range(stub)
    _ = response, mdata  # TODO: add some tests here


# test_get_obs_in_polygon tests that get_obs_in_polygon works as expected.
def test_get_obs_in_polygon(stub, mdata):

    response = get_obs_in_polygon(stub)

    assert len(response.observations) == 1
    obs0 = response.observations[0]

    ts_mdata = obs0.ts_mdata
    assert ts_mdata.version == mdata["version"]
    assert ts_mdata.type == mdata["type"]
    assert ts_mdata.standard_name == mdata["standard_name"]
    assert ts_mdata.unit == mdata["unit"]

    obs_mdata = obs0.obs_mdata
    assert len(obs_mdata) == 1
    obs_mdata0 = obs_mdata[0]
    assert obs_mdata0.value == mdata["value"]


# test_get_obs_in_circle tests that get_obs_in_circle works as expected.
def test_get_obs_in_circle(stub, mdata):

    for test_data in [
        {"lat": mdata["lat"] + 1, "lon": mdata["lon"], "radius": 112, "expected_obs": 1},
        {"lat": mdata["lat"] + 1, "lon": mdata["lon"], "radius": 110, "expected_obs": 0},
    ]:
        response = get_obs_in_circle(
            stub, center=dstore.Point(lat=test_data["lat"], lon=test_data["lon"]), radius=test_data["radius"]
        )
        assert len(response.observations) == test_data["expected_obs"]


if __name__ == "__main__":

    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        stub = dstore_grpc.DatastoreStub(channel)

        # set up general metadata
        mdata = {
            "version": "version_dummy",
            "type": "type_dummy",
            "standard_name": "air_temperature",
            "unit": "celsius",
            "value": "12.7",
            "title": "Air Temperature",
            "instrument": "test",
            "level": "2.0",
            "function": "point",
            "period": 0,
            "lat": 59.91,
            "lon": 10.75,
        }
        mdata["parameter_name"] = "_".join([mdata["standard_name"], mdata["level"], mdata["function"], mdata["period"]])

        # run tests
        test_put_obs(stub, mdata)
        test_get_obs_in_time_range(stub, mdata)
        test_get_obs_in_polygon(stub, mdata)
        test_get_obs_in_circle(stub, mdata)
