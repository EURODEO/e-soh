import json
from typing import Annotated

import datastore_pb2 as dstore
import formatters
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Path
from fastapi import Query
from geojson_pydantic import Feature
from geojson_pydantic import FeatureCollection
from grpc_getter import get_obs_request
from grpc_getter import get_spatial_extent
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import select_autoescape
from routers.items_request_model import ItemsQueryParams
from shapely import geometry
from utilities import get_datetime_range

router = APIRouter(prefix="/collections/observations")

env = Environment(loader=FileSystemLoader("templates"), autoescape=select_autoescape())


@router.get(
    "/items", tags=["Collection items"], response_model=Feature | FeatureCollection, response_model_exclude_none=True
)
async def search_timeseries(parameters: Annotated[ItemsQueryParams, Depends()]):
    if parameters.bbox:
        left, bottom, right, top = map(str.strip, parameters.bbox.split(","))
        poly = geometry.Polygon([(left, bottom), (right, bottom), (right, top), (left, top)])
    if parameters.datetime:
        range = get_datetime_range(parameters.datetime)

    obs_request = dstore.GetObsRequest(
        filter=dict(
            metadata_id=dstore.Strings(values=parameters.ids.split(",") if parameters.ids else None),
            parameter_name=dstore.Strings(
                values=parameters.parameter_name.split(",") if parameters.parameter_name else None
            ),
            naming_authority=dstore.Strings(
                values=parameters.naming_authority.split(",") if parameters.naming_authority else None
            ),
            institution=dstore.Strings(values=parameters.institution.split(",") if parameters.institution else None),
            platform=dstore.Strings(values=parameters.platform.split(",") if parameters.platform else None),
            standard_name=dstore.Strings(
                values=parameters.standard_name.split(",") if parameters.standard_name else None
            ),
            unit=dstore.Strings(values=parameters.unit.split(",") if parameters.unit else None),
            instrument=dstore.Strings(values=parameters.instrument.split(",") if parameters.instrument else None),
            level=dstore.Strings(values=parameters.level.split(",") if parameters.level else None),
            period=dstore.Strings(values=parameters.period.split(",") if parameters.period else None),
            function=dstore.Strings(values=parameters.function.split(",") if parameters.function else None),
        ),
        spatial_area=(
            dstore.Polygon(points=[dstore.Point(lat=coord[1], lon=coord[0]) for coord in poly.exterior.coords])
            if parameters.bbox
            else None
        ),
        temporal_interval=(dstore.TimeInterval(start=range[0], end=range[1]) if parameters.datetime else None),
        temporal_mode="latest",
    )

    time_series = await get_obs_request(obs_request)

    return formatters.metadata_formatters[parameters.f](time_series)


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
            [extent.spatial_extent.left, extent.spatial_extent.bottom],
            [extent.spatial_extent.right, extent.spatial_extent.bottom],
            [extent.spatial_extent.right, extent.spatial_extent.top],
            [extent.spatial_extent.left, extent.spatial_extent.top],
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
