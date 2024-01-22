# For developing:    uvicorn main:app --reload
import datastore_pb2 as dstore

from covjson_pydantic.coverage import Coverage
from covjson_pydantic.coverage import CoverageCollection

# from edr_pydantic.collections import Collection
# from edr_pydantic.collections import Collections
from fastapi import APIRouter

# from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from geojson_pydantic import Feature
from geojson_pydantic import FeatureCollection
from geojson_pydantic import Point
from shapely import buffer
from shapely import geometry
from shapely import wkt


import formatters
from dependencies import get_datetime_range
from grpc_getter import getObsRequest

router = APIRouter(prefix="/collections/observations")

edr_formatter = formatters.get_EDR_formatters()


@router.get(
    "/locations",
    tags=["Collection data queries"],
    response_model=FeatureCollection,
    response_model_exclude_none=True,
)
# We can currently only query data, even if we only need metadata like for this endpoint
# Maybe it would be better to only query a limited set of data instead of everything (meaning 24 hours)
async def get_locations(bbox: str = Query(..., example="5.0,52.0,6.0,52.1")) -> FeatureCollection:  # Hack to use string
    left, bottom, right, top = map(str.strip, bbox.split(","))
    poly = geometry.Polygon([(left, bottom), (right, bottom), (right, top), (left, top)])
    ts_request = dstore.GetObsRequest(
        instruments=["tn"],  # Hack
        inside=dstore.Polygon(points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]),
    )

    ts_response = await getObsRequest(ts_request)
    print(ts_response)
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


@router.get(
    "/locations/{location_id}",
    tags=["Collection data queries"],
    response_model=Coverage,
    response_model_exclude_none=True,
)
async def get_data_location_id(
    location_id: str = Path(..., example="06260"),
    parameter_name: str = Query(..., alias="parameter-name", example="dd,ff,rh,pp,tn"),
    datetime: str | None = None,
    f: str = Query(default="covjson", alias="f", description="Specify return format."),
):
    # TODO: There is no error handling of any kind at the moment!
    #  This is just a quick and dirty demo
    range = get_datetime_range(datetime)
    get_obs_request = dstore.GetObsRequest(
        platforms=[location_id],
        instruments=list(map(str.strip, parameter_name.split(","))),
        interval=dstore.TimeInterval(start=range[0], end=range[1]) if range else None,
    )
    response = await getObsRequest(get_obs_request)
    return edr_formatter[f](response)


@router.get(
    "/position",
    tags=["Collection data queries"],
    response_model=Coverage | CoverageCollection,
    response_model_exclude_none=True,
)
async def get_data_position(
    coords: str = Query(..., example="POINT(5.179705 52.0988218)"),
    parameter_name: str = Query(..., alias="parameter-name", example="dd,ff,rh,pp,tn"),
    datetime: str | None = None,
    f: str = Query(default="covjson", alias="f", description="Specify return format."),
):
    point = wkt.loads(coords)
    assert point.geom_type == "Point"
    poly = buffer(point, 0.0001, quad_segs=1)  # Roughly 10 meters around the point
    return await get_data_area(poly.wkt, parameter_name, datetime, f)


@router.get(
    "/area",
    tags=["Collection data queries"],
    response_model=Coverage | CoverageCollection,
    response_model_exclude_none=True,
)
async def get_data_area(
    coords: str = Query(..., example="POLYGON((5.0 52.0, 6.0 52.0,6.0 52.1,5.0 52.1, 5.0 52.0))"),
    parameter_name: str = Query(..., alias="parameter-name", example="dd,ff,rh,pp,tn"),
    datetime: str | None = None,
    f: str = Query(default="covjson", alias="f", description="Specify return format."),
):
    poly = wkt.loads(coords)
    assert poly.geom_type == "Polygon"
    range = get_datetime_range(datetime)
    get_obs_request = dstore.GetObsRequest(
        standard_names=list(map(str.strip, parameter_name.split(","))),
        inside=dstore.Polygon(points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]),
        interval=dstore.TimeInterval(start=range[0], end=range[1]) if range else None,
    )
    coverages = await getObsRequest(get_obs_request)
    coverages = edr_formatter[f].convert(coverages)
    return coverages
