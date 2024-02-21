# For developing:    uvicorn main:app --reload
from typing import Annotated

import datastore_pb2 as dstore
import formatters
from covjson_pydantic.coverage import Coverage
from covjson_pydantic.coverage import CoverageCollection
from dependencies import get_datetime_range
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from geojson_pydantic import Feature
from geojson_pydantic import FeatureCollection
from geojson_pydantic import Point
from grpc_getter import getObsRequest
from shapely import buffer
from shapely import geometry
from shapely import wkt
from shapely.errors import GEOSException

# from dependencies import verify_parameter_names

router = APIRouter(prefix="/collections/observations")


@router.get(
    "/locations",
    tags=["Collection data queries"],
    response_model=FeatureCollection,
    response_model_exclude_none=True,
)
# We can currently only query data, even if we only need metadata like for this endpoint
# Maybe it would be better to only query a limited set of data instead of everything (meaning 24 hours)
async def get_locations(
    bbox: Annotated[str, Query(example="5.0,52.0,6.0,52.1")]
) -> FeatureCollection:  # Hack to use string
    left, bottom, right, top = map(str.strip, bbox.split(","))
    poly = geometry.Polygon([(left, bottom), (right, bottom), (right, top), (left, top)])
    ts_request = dstore.GetObsRequest(
        filter=dict(parameter_name=dstore.Strings(values=["air_temperature_2.0_maximum_PT10M"])),  # Hack
        spatial_area=dstore.Polygon(
            points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]
        ),
    )

    ts_response = await getObsRequest(ts_request)
    features = [
        Feature(
            type="Feature",
            id=ts.ts_mdata.platform,
            properties=None,
            geometry=Point(
                type="Point",
                coordinates=(
                    ts.obs_mdata[0].geo_point.lon,
                    ts.obs_mdata[0].geo_point.lat,
                ),
            ),
        )  # HACK: Assume loc the same
        for ts in sorted(ts_response.observations, key=lambda ts: ts.ts_mdata.platform)
    ]
    return FeatureCollection(features=features, type="FeatureCollection")


@router.get(
    "/locations/{location_id}",
    tags=["Collection data queries"],
    response_model=Coverage | CoverageCollection,
    response_model_exclude_none=True,
)
async def get_data_location_id(
    location_id: Annotated[str, Path(example="06260")],
    parameter_name: Annotated[
        str | None,
        Query(
            alias="parameter-name",
            example="wind_from_direction_2.0_mean_PT10M,"
            "wind_speed_2.0_mean_PT10M,"
            "relative_humidity_2.0_mean_PT1M,"
            "air_pressure_at_sea_level_2.0_mean_PT1M,"
            "air_temperature_2.0_minimum_PT10M",
        ),
    ] = None,
    datetime: Annotated[str | None, Query(example="2022-12-31T00:00Z/2023-01-01T00:00Z")] = None,
    f: Annotated[formatters.Formats, Query(description="Specify return format.")] = formatters.Formats.covjson,
):
    # TODO: There is no error handling of any kind at the moment!
    #  This is just a quick and dirty demo
    range = get_datetime_range(datetime)
    if parameter_name:
        parameter_name = parameter_name.split(",")
        parameter_name = list(map(lambda x: x.strip(), parameter_name))
    # parameter_name = verify_parameter_names(parameter_name) # should the api verify that the parameter name is valid?
    get_obs_request = dstore.GetObsRequest(
        filter=dict(
            parameter_name=dstore.Strings(values=parameter_name),
            platform=dstore.Strings(values=[location_id]),
        ),
        temporal_interval=(dstore.TimeInterval(start=range[0], end=range[1]) if range else None),
    )
    response = await getObsRequest(get_obs_request)
    return formatters.formatters[f](response)


@router.get(
    "/position",
    tags=["Collection data queries"],
    response_model=Coverage | CoverageCollection,
    response_model_exclude_none=True,
)
async def get_data_position(
    coords: Annotated[str, Query(example="POINT(5.179705 52.0988218)")],
    parameter_name: Annotated[
        str | None,
        Query(
            alias="parameter-name",
            example="wind_from_direction_2.0_mean_PT10M,"
            "wind_speed_2.0_mean_PT10M,"
            "relative_humidity_2.0_mean_PT1M,"
            "air_pressure_at_sea_level_2.0_mean_PT1M,"
            "air_temperature_2.0_minimum_PT10M",
        ),
    ] = None,
    datetime: Annotated[str | None, Query(example="2022-12-31T00:00Z/2023-01-01T00:00Z")] = None,
    f: Annotated[formatters.Formats, Query(description="Specify return format.")] = formatters.Formats.covjson,
):
    try:
        point = wkt.loads(coords)
        if point.geom_type != "Point":
            raise TypeError
        poly = buffer(point, 0.0001, quad_segs=1)  # Roughly 10 meters around the point
    except GEOSException:
        raise HTTPException(
            status_code=400,
            detail={"coords": f"Invalid or unparseable wkt provided: {coords}"},
        )
    except TypeError:
        raise HTTPException(
            status_code=400,
            detail={"coords": f"Invalid geometric type: {point.geom_type}"},
        )
    except Exception:
        raise HTTPException(
            status_code=400,
            detail={"coords": f"Unexpected error occurred during wkt parsing: {coords}"},
        )

    return await get_data_area(coords=poly.wkt, parameter_name=parameter_name, datetime=datetime, f=f)


@router.get(
    "/area",
    tags=["Collection data queries"],
    response_model=Coverage | CoverageCollection,
    response_model_exclude_none=True,
)
async def get_data_area(
    coords: Annotated[str, Query(example="POLYGON((5.0 52.0, 6.0 52.0,6.0 52.1,5.0 52.1, 5.0 52.0))")],
    parameter_name: Annotated[
        str | None,
        Query(
            alias="parameter-name",
            example="wind_from_direction_2.0_mean_PT10M,"
            "wind_speed_2.0_mean_PT10M,"
            "relative_humidity_2.0_mean_PT1M,"
            "air_pressure_at_sea_level_2.0_mean_PT1M,"
            "air_temperature_2.0_minimum_PT10M",
        ),
    ] = None,
    datetime: Annotated[str | None, Query(example="2022-12-31T00:00Z/2023-01-01T00:00Z")] = None,
    f: Annotated[formatters.Formats, Query(description="Specify return format.")] = formatters.Formats.covjson,
):
    try:
        poly = wkt.loads(coords)
        if poly.geom_type != "Polygon":
            raise TypeError
    except GEOSException:
        raise HTTPException(
            status_code=400,
            detail={"coords": f"Invalid or unparseable wkt provided: {coords}"},
        )
    except TypeError:
        raise HTTPException(
            status_code=400,
            detail={"coords": f"Invalid geometric type: {poly.geom_type}"},
        )
    except Exception:
        raise HTTPException(
            status_code=400,
            detail={"coords": f"Unexpected error occurred during wkt parsing: {coords}"},
        )

    range = get_datetime_range(datetime)
    # await verify_parameter_names(parameter_name)
    get_obs_request = dstore.GetObsRequest(
        filter=dict(parameter_name=dstore.Strings(values=parameter_name.split(",") if parameter_name else None)),
        spatial_area=dstore.Polygon(
            points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]
        ),
        temporal_interval=(dstore.TimeInterval(start=range[0], end=range[1]) if range else None),
    )
    coverages = await getObsRequest(get_obs_request)
    coverages = formatters.formatters[f](coverages)
    return coverages
