# Run with:
# For developing:    uvicorn main:app --reload --bind=0.0.0.0:8000

import os
from datetime import datetime
from datetime import timezone
from typing import Annotated

from brotli_asgi import BrotliMiddleware

from covjson_pydantic.ndarray import NdArray
from fastapi import FastAPI
from fastapi import Query

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import AwareDatetime

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc

from covjson_pydantic.coverage import Coverage
from covjson_pydantic.domain import Domain, DomainType, Axes, ValuesAxis

app = FastAPI()
app.add_middleware(BrotliMiddleware)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get(
    "/collections/observations/locations/{location_id}",
    response_model=Coverage,
    response_model_exclude_none=True,)
def read_item(location_id: str, parameter_name: str = Query(..., alias="parameter-name")):
    # TODO: There is no error handling of any kind at the moment! This is just a quick and dirty demo
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        grpc_stub = dstore_grpc.DatastoreStub(channel)
        ts_request = dstore.FindTSRequest(
            station_ids=[location_id],
            param_ids=list(map(str.strip, parameter_name.split(",")))
        )
        ts_response = grpc_stub.FindTimeSeries(ts_request)
        assert len(ts_response.tseries) == 1
        # ts_id = ts_response.tseries[0].id

        from_time = Timestamp()
        from_time.FromDatetime(datetime(2022, 12, 31))   # TODO: Get from request
        to_time = Timestamp()
        to_time.FromDatetime(datetime(2023, 11, 1))      # TODO: Get from request
        request = dstore.GetObsRequest(
            tsids=[ts.id for ts in ts_response.tseries],
            fromtime=from_time,
            totime=to_time,
        )
        response = grpc_stub.GetObservations(request)

        assert len(response.tsobs) == 1
        assert response.tsobs[0].tsid == ts_response.tseries[0].id

        # Collect data
        lat = ts_response.tseries[0].metadata.pos.lat
        lon = ts_response.tseries[0].metadata.pos.lon
        tuples = ((o.time.ToDatetime().replace(tzinfo=timezone.utc), o.value) for o in response.tsobs[0].obs)
        (times, values) = zip(*tuples)

        domain = Domain(domainType=DomainType.point_series,
                        axes=Axes(x=ValuesAxis[float](values=[lon]),
                                  y=ValuesAxis[float](values=[lat]),
                                  t=ValuesAxis[AwareDatetime](values=times)))
        nd_array = NdArray(values=values, axisNames=["t", "y", "x"], shape=[len(values), 1, 1])

        cov = Coverage(domain=domain, ranges={"rh": nd_array})  # TODO: Not hardcoded!

    return cov