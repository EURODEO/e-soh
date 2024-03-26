# For developing:    uvicorn main:app --reload
from collections import defaultdict
from typing import Annotated
from typing import DefaultDict
from typing import Dict
from typing import Set
from typing import Tuple

import datastore_pb2 as dstore
import formatters
from covjson_pydantic.coverage import Coverage
from covjson_pydantic.coverage import CoverageCollection
from covjson_pydantic.parameter import Parameter
from custom_geo_json.edr_feature_collection import EDRFeatureCollection
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from formatters.covjson import make_parameter
from geojson_pydantic import Feature
from geojson_pydantic import Point
from grpc_getter import get_obs_request
from shapely import buffer
from shapely import geometry
from shapely import wkt
from shapely.errors import GEOSException
from utilities import get_datetime_range
from utilities import split_and_strip
from utilities import validate_bbox
from utilities import verify_parameter_names

router = APIRouter(prefix="/collections/observations")

response_fields_needed_for_data_api = [
    "parameter_name",
    "platform",
    "geo_point",
    "standard_name",
    "level",
    "period",
    "function",
    "unit",
    "obstime_instant",
    "value",
]


@router.get(
    "/locations",
    tags=["Collection data queries"],
    response_model=EDRFeatureCollection,
    response_model_exclude_none=True,
)
# We can currently only query data, even if we only need metadata like for this endpoint
# Maybe it would be better to only query a limited set of data instead of everything (meaning 24 hours)
async def get_locations(
    bbox: Annotated[str | None, Query(example="5.0,52.0,6.0,52.1")] = None
) -> EDRFeatureCollection:  # Hack to use string
    ts_request = dstore.GetObsRequest(
        temporal_latest=True,
        included_response_fields=[
            "parameter_name",
            "platform",
            "geo_point",
            "standard_name",
            "unit",
            "level",
            "period",
            "function",
        ],
    )
    # Add spatial area to the time series request if bbox exists.
    if bbox:
        left, bottom, right, top = validate_bbox(bbox)
        poly = geometry.Polygon([(left, bottom), (right, bottom), (right, top), (left, top)])
        ts_request.spatial_area.points.extend(
            [dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords],
        )

    ts_response = await get_obs_request(ts_request)

    platform_parameters: DefaultDict[str, Set[str]] = defaultdict(set)
    platform_coordinates: Dict[str, Set[Tuple[float, float]]] = defaultdict(set)
    all_parameters: Dict[str, Parameter] = {}
    for obs in ts_response.observations:
        parameter = make_parameter(obs.ts_mdata)
        platform_parameters[obs.ts_mdata.platform].add(obs.ts_mdata.parameter_name)
        # Take last point
        platform_coordinates[obs.ts_mdata.platform].add(
            (obs.obs_mdata[-1].geo_point.lon, obs.obs_mdata[-1].geo_point.lat)
        )
        # Check for inconsistent parameter definitions between stations
        # TODO: How to handle those?
        if obs.ts_mdata.parameter_name in all_parameters and all_parameters[obs.ts_mdata.parameter_name] != parameter:
            raise HTTPException(
                status_code=500,
                detail={
                    "parameter": f"Parameter with name {obs.ts_mdata.parameter_name} "
                    f"has multiple definitions:\n{all_parameters[obs.ts_mdata.parameter_name]}\n{parameter}"
                },
            )
        all_parameters[obs.ts_mdata.parameter_name] = parameter

    # Check for multiple coordinates on one station
    for station_id in platform_parameters.keys():
        if len(platform_coordinates[station_id]) > 1:
            raise HTTPException(
                status_code=500,
                detail={
                    "coordinates": f"Station with id `{station_id} "
                    f"has multiple coordinates: {platform_coordinates[station_id]}"
                },
            )

    features = [
        Feature(
            type="Feature",
            id=station_id,
            properties={
                # TODO: Change to platform_name to correct one when its available, this is only for geoweb demo
                "name": f"platform-{station_id}",
                "detail": f"https://oscar.wmo.int/surface/#/search/station/stationReportDetails/{station_id}",
                "parameter-name": sorted(platform_parameters[station_id]),
            },
            geometry=Point(
                type="Point",
                coordinates=list(platform_coordinates[station_id])[0],
            ),
        )
        for station_id in sorted(platform_parameters.keys())  # Sort by station_id
    ]
    parameters = {parameter_id: all_parameters[parameter_id] for parameter_id in sorted(all_parameters)}

    return EDRFeatureCollection(features=features, type="FeatureCollection", parameters=parameters)


@router.get(
    "/locations/{location_id}",
    tags=["Collection data queries"],
    response_model=Coverage | CoverageCollection,
    response_model_exclude_none=True,
)
async def get_data_location_id(
    location_id: Annotated[str, Path(example="0-20000-0-06260")],
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
    request = dstore.GetObsRequest(
        filter=dict(
            platform=dstore.Strings(values=[location_id]),
        ),
        included_response_fields=response_fields_needed_for_data_api,
    )

    if parameter_name:
        parameter_name = split_and_strip(parameter_name)
        await verify_parameter_names(parameter_name)
        request.filter["parameter_name"].values.extend(parameter_name)

    if datetime:
        start, end = get_datetime_range(datetime)
        request.temporal_interval.start.CopyFrom(start)
        request.temporal_interval.end.CopyFrom(end)

    response = await get_obs_request(request)
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

    request = dstore.GetObsRequest(
        spatial_area=dstore.Polygon(
            points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords]
        ),
        included_response_fields=response_fields_needed_for_data_api,
    )

    if parameter_name:
        parameter_name = split_and_strip(parameter_name)
        await verify_parameter_names(parameter_name)
        request.filter["parameter_name"].values.extend(parameter_name)

    if datetime:
        start, end = get_datetime_range(datetime)
        request.temporal_interval.start.CopyFrom(start)
        request.temporal_interval.end.CopyFrom(end)

    coverages = await get_obs_request(request)
    coverages = formatters.formatters[f](coverages)
    return coverages
