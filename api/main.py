# Run with:
# For load testing:  gunicorn main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker
# For developing:    uvicorn main:app --reload

import os
from datetime import datetime
from datetime import timezone

from covjson_pydantic.ndarray import NdArray
from fastapi import FastAPI

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import AwareDatetime

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc

from covjson_pydantic.coverage import Coverage
from covjson_pydantic.domain import Domain, DomainType, Axes, ValuesAxis

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get(
    "/collections/observations/locations/{location_id}",
    response_model=Coverage,
    response_model_exclude_none=True,)
def read_item(location_id: str):
    # TODO: There is no error handling of any kind at the moment! This is just a quick and dirty demo
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        grpc_stub = dstore_grpc.DatastoreStub(channel)
        ts_request = dstore.FindTSRequest(
            station_ids=[location_id],
            param_ids=["rh"]   # TODO: Get from request
        )
        ts_response = grpc_stub.FindTimeSeries(ts_request)
        assert len(ts_response.tseries) == 1
        ts_id = ts_response.tseries[0].id

        from_time = Timestamp()
        from_time.FromDatetime(datetime(2022, 12, 31))   # TODO: Get from request
        to_time = Timestamp()
        to_time.FromDatetime(datetime(2023, 11, 1))      # TODO: Get from request
        request = dstore.GetObsRequest(
            tsids=[ts_id],
            fromtime=from_time,
            totime=to_time,
        )
        response = grpc_stub.GetObservations(request)

        assert len(response.tsobs) == 1
        assert response.tsobs[0].tsid == ts_id

        # Collect data
        lat = ts_response.tseries[0].metadata.pos.lat
        lon = ts_response.tseries[0].metadata.pos.lon
        times = []
        values = []
        for o in response.tsobs[0].obs:
            dt = o.time.ToDatetime().replace(tzinfo=timezone.utc)
            times.append(dt)
            values.append(o.value)

        domain = Domain(domainType=DomainType.point_series,
                        axes=Axes(x=ValuesAxis[float](values=[lon]),
                                  y=ValuesAxis[float](values=[lat]),
                                  t=ValuesAxis[AwareDatetime](values=times)))
        nd_array = NdArray(values=values, axisNames=["t", "y", "x"], shape=[len(values), 1, 1])

        cov = Coverage(domain=domain, ranges={"rh": nd_array})  # TODO: Not hardcoded!

    return cov