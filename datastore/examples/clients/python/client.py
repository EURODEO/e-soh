#!/usr/bin/env python3
# tested with Python 3.11
# Generate protobuf code with following command from top level directory:
# python -m grpc_tools.protoc --proto_path=datastore/protobuf datastore.proto --python_out=examples/clients/python --grpc_python_out=examples/clients/python  # noqa: E501
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


# callPutObs demonstrates how to insert observations in the datastore.
def call_put_obs(
    stub, version, type, standard_name, unit, value, title, instrument, level, function, period, parameter_name
):
    ts_mdata = dstore.TSMetadata(
        version=version,
        type=type,
        standard_name=standard_name,
        unit=unit,
        title=title,
        instrument=instrument,
        level=level,
        function=function,
        period=period,
        parameter_name=parameter_name
        # add more attributes as required ...
    )

    obs_mdata = dstore.ObsMetadata(
        id="id_dummy",
        geo_point=dstore.Point(
            lat=59.91,
            lon=10.75,
        ),
        pubtime=dtime2tstamp(datetime(2023, 1, 1, 0, 0, 10, 0, tzinfo=timezone.utc)),
        data_id="data_id_dummy",
        obstime_instant=dtime2tstamp(datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)),
        value=value,
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
    response = stub.PutObservations(request)

    return response


# callGetObsInTimeRange demonstrates how to retrieve from the datastore all observations in an
# obs time range.
def call_get_obs_in_time_range(stub):
    request = dstore.GetObsRequest(
        temporal_interval=dstore.TimeInterval(
            start=dtime2tstamp(datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)),
            end=dtime2tstamp(datetime(2023, 1, 2, 0, 0, 0, 0, tzinfo=timezone.utc)),
        )
    )
    response = stub.GetObservations(request)

    return response


# callGetObsInPolygon demonstrates how to retrieve from the datastore all observations in a
# polygon.
def call_get_obs_in_polygon(stub):
    points = []
    points.append(dstore.Point(lat=59.90, lon=10.70))
    points.append(dstore.Point(lat=59.90, lon=10.80))
    points.append(dstore.Point(lat=60, lon=10.80))
    points.append(dstore.Point(lat=60, lon=10.70))

    request = dstore.GetObsRequest(spatial_area=dstore.Polygon(points=points))
    response = stub.GetObservations(request)

    return response


if __name__ == "__main__":
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        stub = dstore_grpc.DatastoreStub(channel)

        version = "version_dummy"
        type = "type_dummy"
        standard_name = "air_temperature"
        unit = "celsius"
        value = "12.7"
        title = "Air Temperature"
        instrument = "test"
        level = "2.0"
        function = "point"
        period = "PT0S"
        parameter_name = "_".join([standard_name, level, function, period])

        response = call_put_obs(
            stub, version, type, standard_name, unit, value, title, instrument, level, function, period, parameter_name
        )
        print("response from call_put_obs: {}".format(response))

        response = call_get_obs_in_time_range(stub)
        print("response from call_get_obs_in_time_range: {}".format(response))

        response = call_get_obs_in_polygon(stub)
        print("response from call_get_obs_in_polygon: {}".format(response))

        assert len(response.observations) == 1
        obs0 = response.observations[0]

        ts_mdata = obs0.ts_mdata
        assert ts_mdata.version == version
        assert ts_mdata.type == type
        assert ts_mdata.standard_name == standard_name
        assert ts_mdata.unit == unit

        obs_mdata = obs0.obs_mdata
        assert len(obs_mdata) == 1
        obs_mdata0 = obs_mdata[0]
        assert obs_mdata0.value == value
