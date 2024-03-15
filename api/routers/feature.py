from typing import Annotated

import datastore_pb2 as dstore
import formatters
from fastapi import APIRouter
from fastapi import Path
from fastapi import Query
from geojson_pydantic import Feature
from geojson_pydantic import FeatureCollection
from grpc_getter import get_obs_request
from grpc_getter import get_spatial_extent
from shapely import geometry
from utilities import get_datetime_range

router = APIRouter(prefix="/collections/observations")


@router.get(
    "/items", tags=["Collection items"], response_model=Feature | FeatureCollection, response_model_exclude_none=True
)
async def search_timeseries(
    bbox: Annotated[str | None, Query(example="5.0,52.0,6.0,52.1")],
    datetime: Annotated[
        str | None,
        Query(
            example="2022-12-31T00:00Z/2023-01-01T00:00Z",
            description="E-SOH database only contains data from the last 24 hours",
        ),
    ] = None,
    ids: Annotated[str | None, Query(description="Comma separated list of time series ids")] = None,
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
    if bbox:
        left, bottom, right, top = map(str.strip, bbox.split(","))
        poly = geometry.Polygon([(left, bottom), (right, bottom), (right, top), (left, top)])
    if datetime:
        range = get_datetime_range(datetime)

    obs_request = dstore.GetObsRequest(
        filter=dict(
            metadata_id=dstore.Strings(values=ids.split(",") if ids else None),
            parameter_name=dstore.Strings(values=parameter_name.split(",") if parameter_name else None),
            naming_authority=dstore.Strings(values=naming_authority.split(",") if naming_authority else None),
            institution=dstore.Strings(values=institution.split(",") if institution else None),
            platform=dstore.Strings(values=platform.split(",") if platform else None),
            standard_name=dstore.Strings(values=standard_name.split(",") if standard_name else None),
            unit=dstore.Strings(values=unit.split(",") if unit else None),
            instrument=dstore.Strings(values=instrument.split(",") if instrument else None),
            level=dstore.Strings(values=level.split(",") if level else None),
            period=dstore.Strings(values=period.split(",") if period else None),
            function=dstore.Strings(values=function.split(",") if function else None),
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
    print(extent.temporal_extent.start)
    dataset_metadata = {
        "id": "what is E-SOH data set ID?",
        "conformsTo": ["http://wis.wmo.int/spec/wcmp/2/conf/core"],
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [extent.spatial_extent.left, extent.spatial_extent.bottom],
                [extent.spatial_extent.right, extent.spatial_extent.bottom],
                [extent.spatial_extent.right, extent.spatial_extent.top],
                [extent.spatial_extent.left, extent.spatial_extent.top],
            ],
        },
        "time": {
            "interval": [extent.temporal_extent.start.ToDatetime(), extent.temporal_extent.end.ToDatetime()],
            "resolution": "PT10M",
        },
        "properties": {
            "title": "Additional and sub-hourly observations from European area,"
            " including synoptic observations distributed over GTS",
            "description": "collection of observations from weather stations situated in European countries."
            " Data from synoptic stations and additional stations from 3rd party station holders is distributed"
            " with best possible time resolution.  All parameters resolved in the CF-standards could be distributed."
            " The number of parameters differs pr station. Observations from the last 24hours is available for"
            " download from EDR-api. Timestamp for each observations is based on observation time, or the end of a"
            " aggregation period. Depending om the method used to produce the parameter. "
            "All timestamps is given as UTC. Timeliness of the data depends on when it is distributed from the NMS."
            " Timeliness within the system is 1 minute after reception time at the E-soh ingestor."
            " Observations in E-soh is initial received at one of the NWS'es."
            " The NMS is responsible for checking the quality of the observations. "
            "The NMS is also responsible for collection and distribution of metadata compliant to E-soh and WIS-2.0."
            " E-soh also include all global observations distributed over the Global Transition System (GTS)"
            " that is considered as open and free for use. E-soh aims to serve all data according to FAIR principles."
            " For questions or technical documentation, you can go to https://?????????",
            "themes": [
                {
                    "concepts": [{"id": "meteorology"}],
                    "scheme": "http://wis.wmo.int/2012/codelists/WMOCodeLists#WMO_CategoryCode",
                },
                {
                    "concepts": [
                        {"id": "Surface observations"},
                    ],
                    "scheme": "https://github.com/wmo-im/"
                    "topic-hierarchy/earth-system-discipline/weather/surface-based-observations/index.csv",
                },
                {
                    "concepts": [{"id": "weather"}],
                    "scheme": "https://github.com/wmo-im/"
                    "wis2-topic-hierarchy/blob/main/topic-hierarchy/earth-system-discipline/index.csv",
                },
                {
                    "concepts": [{"id": "continual"}],
                    "scheme": "https://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_FrequencyCode",
                },
            ],
        },
        "links": [
            {
                "rel": "items",
                "href": "E-SOH dataset mqtt stream",
                "title": "E-SOH dataset data notifications",
                "type": "application/json",
            },
            {
                "rel": "items",
                "href": "E-SOH time series mqtt stream",
                "title": "E-SOH time series data notifications",
                "type": "application/json",
            },
            {
                "rel": "data",
                "href": "E-SOH API landing page",
                "title": "E-SOH EDR API landing page",
                "type": "application/json",
            },
            {
                "rel": "related",
                "href": "E-SOH API documentation",
                "title": "E-SOH API documentation",
                "type": "application/json",
            },
            {
                "rel": "license",
                "href": "need to agree on a license",
                "title": "need to agree on license",
                "type": "text/html",
            },
        ],
    }
    return dataset_metadata
