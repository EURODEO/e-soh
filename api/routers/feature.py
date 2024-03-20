import json
from typing import Annotated

import datastore_pb2 as dstore
import formatters
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from geojson_pydantic import Feature
from geojson_pydantic import FeatureCollection
from grpc_getter import get_obs_request
from grpc_getter import get_spatial_extent
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import select_autoescape
from shapely import geometry
from utilities import get_datetime_range
from utilities import split_and_strip

router = APIRouter(prefix="/collections/observations")

env = Environment(loader=FileSystemLoader("templates"), autoescape=select_autoescape())


@router.get(
    "/items", tags=["Collection items"], response_model=Feature | FeatureCollection, response_model_exclude_none=True
)
async def search_timeseries(
    bbox: Annotated[str | None, Query(example="5.0,52.0,6.0,52.1")] = None,
    datetime: Annotated[
        str | None,
        Query(
            example="2022-12-31T00:00Z/2023-01-01T00:00Z",
            description="E-SOH database only contains data from the last 24 hours",
        ),
    ] = None,
    ids: Annotated[str | None, Query(description="List of time series ids")] = None,
    parameter_name: Annotated[str | None, Query(alias="parameter-name", description="E-SOH parameter name")] = None,
    naming_authority: Annotated[
        str | None,
        Query(alias="naming-authority", description="Naming authority that created the data", example="no.met"),
    ] = None,
    institution: Annotated[
        str | None,
        Query(description="Institution that published the data", example="Norwegian meterological institution"),
    ] = None,
    platform: Annotated[
        str | None, Query(description="Platform ID, WIGOS or WIGOS equivalent.", example="0-20000-0-01492")
    ] = None,
    standard_name: Annotated[
        str | None, Query(alias="standard-name", description="CF 1.9 standard name", example="air_temperature")
    ] = None,
    unit: Annotated[str | None, Query(description="Unit of observed physical property", example="degC")] = None,
    instrument: Annotated[str | None, Query(description="Instrument Id")] = None,
    level: Annotated[
        str | None,
        Query(description="Instruments height above ground or distance below surface, in meters", example=2),
    ] = None,
    period: Annotated[
        str | None, Query(description="Duration of collection period in ISO8601", example="PT10M")
    ] = None,
    function: Annotated[
        str | None, Query(description="Aggregation function used to sample observed property", example="maximum")
    ] = None,
    f: Annotated[
        formatters.Metadata_Formats, Query(description="Specify return format")
    ] = formatters.Metadata_Formats.geojson,
):
    if not bbox and not platform:
        raise HTTPException(400, detail="Have to set at least one of bbox or platform.")
    if bbox:
        left, bottom, right, top = map(str.strip, bbox.split(","))
        poly = geometry.Polygon([(left, bottom), (right, bottom), (right, top), (left, top)])
    if datetime:
        range = get_datetime_range(datetime)

    obs_request = dstore.GetObsRequest(
        filter=dict(
            metadata_id=dstore.Strings(values=split_and_strip(ids) if ids else None),
            parameter_name=dstore.Strings(values=split_and_strip(parameter_name) if parameter_name else None),
            naming_authority=dstore.Strings(values=split_and_strip(naming_authority) if naming_authority else None),
            institution=dstore.Strings(values=split_and_strip(institution) if institution else None),
            platform=dstore.Strings(values=split_and_strip(platform) if platform else None),
            standard_name=dstore.Strings(values=split_and_strip(standard_name) if standard_name else None),
            unit=dstore.Strings(values=split_and_strip(unit) if unit else None),
            instrument=dstore.Strings(values=split_and_strip(instrument) if instrument else None),
            level=dstore.Strings(values=split_and_strip(level) if level else None),
            period=dstore.Strings(values=split_and_strip(period) if period else None),
            function=dstore.Strings(values=split_and_strip(function) if function else None),
        ),
        spatial_area=(
            dstore.Polygon(points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords])
            if bbox
            else None
        ),
        temporal_interval=(dstore.TimeInterval(start=range[0], end=range[1]) if datetime else None),
        temporal_mode="latest",
    )

    time_series = await get_obs_request(obs_request)

    return formatters.metadata_formatters[f](time_series)


@router.get("/items/{item_id}", tags=["Collection items"], response_model=Feature, response_model_exclude_none=True)
async def get_time_series_by_id(
    item_id: Annotated[str, Path()],
    f: Annotated[
        formatters.Metadata_Formats, Query(description="Specify return format")
    ] = formatters.Metadata_Formats.geojson,
):
    obs_request = dstore.GetObsRequest(filter=dict(metadata_id=dstore.Strings(values=[item_id])))
    time_series = await get_obs_request(obs_request)

    return formatters.metadata_formatters[f](time_series)


@router.get("/dataset", tags=["E-SOH dataset"], include_in_schema=False)
async def get_dataset_metadata():
    # need to get spatial extent.
    spatial_request = dstore.GetExtentsRequest()
    extent = await get_spatial_extent(spatial_request)
    dynamic_fields = {
        "spatial_extents": [
            [
                [extent.spatial_extent.left, extent.spatial_extent.bottom],
                [extent.spatial_extent.right, extent.spatial_extent.bottom],
                [extent.spatial_extent.right, extent.spatial_extent.top],
                [extent.spatial_extent.left, extent.spatial_extent.top],
                [extent.spatial_extent.left, extent.spatial_extent.bottom],
            ]
        ],
        "temporal_extents": [
            [
                f"{extent.temporal_extent.start.ToDatetime().strftime('%Y-%m-%dT%H:%M:%SZ')}",
                f"{extent.temporal_extent.end.ToDatetime().strftime('%Y-%m-%dT%H:%M:%SZ')}",
            ],
        ],
    }

    template = env.get_template("dataset_metadata_template.j2")
    dataset_metadata = template.render(dynamic_fields)
    return json.loads(dataset_metadata)
