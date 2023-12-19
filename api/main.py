# Run with:
# For developing:    uvicorn main:app --reload
import math
import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from itertools import groupby
from typing import Tuple

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
import metadata_endpoints
from brotli_asgi import BrotliMiddleware
from covjson_pydantic.coverage import Coverage
from covjson_pydantic.coverage import CoverageCollection
from covjson_pydantic.domain import Axes
from covjson_pydantic.domain import Domain
from covjson_pydantic.domain import DomainType
from covjson_pydantic.domain import ValuesAxis
from covjson_pydantic.ndarray import NdArray
from covjson_pydantic.observed_property import ObservedProperty
from covjson_pydantic.parameter import Parameter
from covjson_pydantic.reference_system import ReferenceSystem
from covjson_pydantic.reference_system import ReferenceSystemConnectionObject
from covjson_pydantic.unit import Unit
from edr_pydantic.capabilities import LandingPageModel
from edr_pydantic.collections import Collection
from edr_pydantic.collections import Collections
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from fastapi.requests import Request
from geojson_pydantic import Feature
from geojson_pydantic import FeatureCollection
from geojson_pydantic import Point
from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import AwareDatetime
from pydantic import TypeAdapter
from shapely import buffer
from shapely import geometry
from shapely import wkt


app = FastAPI()
app.add_middleware(BrotliMiddleware)


def collect_data(ts_mdata, obs_mdata):
    lat = obs_mdata[0].geo_point.lat  # HACK: For now assume they all have the same position
    lon = obs_mdata[0].geo_point.lon
    tuples = (
        (o.obstime_instant.ToDatetime(tzinfo=timezone.utc), float(o.value)) for o in obs_mdata
    )  # HACK: str -> float
    (times, values) = zip(*tuples)
    param_id = ts_mdata.instrument
    unit = ts_mdata.unit

    return (lat, lon, times), param_id, unit, values


def get_data_for_time_series(get_obs_request):
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        grpc_stub = dstore_grpc.DatastoreStub(channel)
        response = grpc_stub.GetObservations(get_obs_request)

        # Collect data
        coverages = []
        data = [collect_data(md.ts_mdata, md.obs_mdata) for md in response.observations]

        # Need to sort before using groupBy. Also sort on param_id to get consistently sorted output
        data.sort(key=lambda x: (x[0], x[1]))
        # The multiple coverage logic is not needed for this endpoint,
        # but we want to share this code between endpoints
        for (lat, lon, times), group in groupby(data, lambda x: x[0]):
            referencing = [
                ReferenceSystemConnectionObject(
                    coordinates=["y", "x"],
                    system=ReferenceSystem(type="GeographicCRS", id="http://www.opengis.net/def/crs/EPSG/0/4326"),
                ),
                ReferenceSystemConnectionObject(
                    coordinates=["z"],
                    system=ReferenceSystem(type="TemporalRS", calendar="Gregorian"),
                ),
            ]
            domain = Domain(
                domainType=DomainType.point_series,
                axes=Axes(
                    x=ValuesAxis[float](values=[lon]),
                    y=ValuesAxis[float](values=[lat]),
                    t=ValuesAxis[AwareDatetime](values=times),
                ),
                referencing=referencing,
            )

            parameters = {}
            ranges = {}
            for (_, _, _), param_id, unit, values in group:
                if all(math.isnan(v) for v in values):
                    continue  # Drop ranges if completely nan.
                    # TODO: Drop the whole coverage if it becomes empty?
                values_no_nan = [v if not math.isnan(v) else None for v in values]
                # TODO: Improve this based on "standard name", etc.
                parameters[param_id] = Parameter(
                    observedProperty=ObservedProperty(label={"en": param_id}), unit=Unit(label={"en": unit})
                )  # TODO: Also fill symbol?
                ranges[param_id] = NdArray(
                    values=values_no_nan, axisNames=["t", "y", "x"], shape=[len(values_no_nan), 1, 1]
                )

            coverages.append(Coverage(domain=domain, parameters=parameters, ranges=ranges))

        if len(coverages) == 0:
            raise HTTPException(status_code=404, detail="No data found")
        elif len(coverages) == 1:
            return coverages[0]
        else:
            return CoverageCollection(
                coverages=coverages, parameters=coverages[0].parameters
            )  # HACK to take parameters from first one


def get_datetime_range(datetime_string: str | None) -> Tuple[Timestamp, Timestamp] | None:
    if not datetime_string:
        return None

    start_datetime, end_datetime = Timestamp(), Timestamp()
    aware_datetime_type_adapter = TypeAdapter(AwareDatetime)
    datetimes = tuple(value.strip() for value in datetime_string.split("/"))
    if len(datetimes) == 1:
        start_datetime.FromDatetime(aware_datetime_type_adapter.validate_python(datetimes[0]))
        end_datetime.FromDatetime(
            aware_datetime_type_adapter.validate_python(datetimes[0]) + timedelta(seconds=1)
        )  # HACK: Add one second so we get some data, as the store returns [start, end)
    else:
        if datetimes[0] != "..":
            start_datetime.FromDatetime(aware_datetime_type_adapter.validate_python(datetimes[0]))
        else:
            start_datetime.FromDatetime(datetime.min)
        if datetimes[1] != "..":
            # HACK add one second so that the end_datetime is included in the interval.
            end_datetime.FromDatetime(aware_datetime_type_adapter.validate_python(datetimes[1]) + timedelta(seconds=1))
        else:
            end_datetime.FromDatetime(datetime.max)

    return start_datetime, end_datetime


@app.get(
    "/",
    tags=["Capabilities"],
    response_model=LandingPageModel,
    response_model_exclude_none=True,
)
async def landing_page(request: Request) -> LandingPageModel:
    return metadata_endpoints.get_landing_page(request)


@app.get(
    "/collections",
    tags=["Capabilities"],
    response_model=Collections,
    response_model_exclude_none=True,
)
async def get_collections(request: Request) -> Collections:
    return metadata_endpoints.get_collections(request)


@app.get(
    "/collections/observations",
    tags=["Collection metadata"],
    response_model=Collection,
    response_model_exclude_none=True,
)
async def get_collection_metadata(request: Request) -> Collection:
    return metadata_endpoints.get_collection_metadata(request)


@app.get(
    "/collections/observations/locations",
    tags=["Collection data queries"],
    response_model=FeatureCollection,
    response_model_exclude_none=True,
)
# We can currently only query data, even if we only need metadata like for this endpoint
# Maybe it would be better to only query a limited set of data instead of everything (meaning 24 hours)
def get_locations(bbox: str = Query(..., example="5.0,52.0,6.0,52.1")) -> FeatureCollection:  # Hack to use string
    left, bottom, right, top = map(str.strip, bbox.split(","))
    poly = geometry.Polygon([(left, bottom), (right, bottom), (right, top), (left, top)])
    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        grpc_stub = dstore_grpc.DatastoreStub(channel)
        ts_request = dstore.GetObsRequest(
            instrument=["tn"],  # Hack
            inside=dstore.Polygon(points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]),
        )
        ts_response = grpc_stub.GetObservations(ts_request)

        features = [
            Feature(
                type="Feature",
                id=ts.ts_mdata.platform,
                properties=None,
                geometry=Point(
                    type="Point",
                    coordinates=(ts.obs_mdata[0].geo_point.lon, ts.obs_mdata[0].geo_point.lat),
                ),
            )  # HACK: Assume loc the same
            for ts in sorted(ts_response.observations, key=lambda ts: ts.ts_mdata.platform)
        ]
        return FeatureCollection(features=features, type="FeatureCollection")


@app.get(
    "/collections/observations/locations/{location_id}",
    tags=["Collection data queries"],
    response_model=Coverage,
    response_model_exclude_none=True,
)
def get_data_location_id(
    location_id: str = Path(..., example="06260"),
    parameter_name: str = Query(..., alias="parameter-name", example="dd,ff,rh,pp,tn"),
    datetime: str | None = None,
):
    # TODO: There is no error handling of any kind at the moment!
    #  This is just a quick and dirty demo
    range = get_datetime_range(datetime)
    get_obs_request = dstore.GetObsRequest(
        platform=[location_id],
        instrument=list(map(str.strip, parameter_name.split(","))),
        interval=dstore.TimeInterval(start=range[0], end=range[1]) if range else None,
    )
    return get_data_for_time_series(get_obs_request)


@app.get(
    "/collections/observations/position",
    tags=["Collection data queries"],
    response_model=Coverage | CoverageCollection,
    response_model_exclude_none=True,
)
def get_data_position(
    coords: str = Query(..., example="POINT(5.179705 52.0988218)"),
    parameter_name: str = Query(..., alias="parameter-name", example="dd,ff,rh,pp,tn"),
    datetime: str | None = None,
):
    point = wkt.loads(coords)
    assert point.geom_type == "Point"
    poly = buffer(point, 0.0001, quad_segs=1)  # Roughly 10 meters around the point
    return get_data_area(poly.wkt, parameter_name, datetime)


@app.get(
    "/collections/observations/area",
    tags=["Collection data queries"],
    response_model=Coverage | CoverageCollection,
    response_model_exclude_none=True,
)
def get_data_area(
    coords: str = Query(..., example="POLYGON((5.0 52.0, 6.0 52.0,6.0 52.1,5.0 52.1, 5.0 52.0))"),
    parameter_name: str = Query(..., alias="parameter-name", example="dd,ff,rh,pp,tn"),
    datetime: str | None = None,
):
    poly = wkt.loads(coords)
    assert poly.geom_type == "Polygon"
    range = get_datetime_range(datetime)
    get_obs_request = dstore.GetObsRequest(
        instruments=list(map(str.strip, parameter_name.split(","))),
        inside=dstore.Polygon(points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]),
        interval=dstore.TimeInterval(start=range[0], end=range[1]) if range else None,
    )
    return get_data_for_time_series(get_obs_request)
