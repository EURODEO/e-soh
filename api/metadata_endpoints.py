import logging
from typing import Dict

import datastore_pb2 as dstore
from edr_pydantic.capabilities import Contact
from edr_pydantic.capabilities import LandingPageModel
from edr_pydantic.capabilities import Provider
from edr_pydantic.collections import Collection
from edr_pydantic.collections import Collections
from edr_pydantic.data_queries import DataQueries
from edr_pydantic.data_queries import EDRQuery
from edr_pydantic.extent import Extent
from edr_pydantic.extent import Spatial
from edr_pydantic.link import EDRQueryLink
from edr_pydantic.link import Link
from edr_pydantic.observed_property import ObservedProperty
from edr_pydantic.parameter import Parameter
from edr_pydantic.unit import Unit
from edr_pydantic.variables import Variables
from fastapi import HTTPException
from grpc_getter import get_ts_ag_request


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_landing_page(request):
    return LandingPageModel(
        title="E-SOH EDR API",
        description="The E-SOH EDR API",
        keywords=["weather", "temperature", "wind", "humidity", "pressure", "clouds", "radiation"],
        provider=Provider(name="RODEO", url="https://rodeo-project.eu/"),
        contact=Contact(email="rodeoproject@fmi.fi"),
        links=[
            Link(href=f"{request.url}", rel="self", title="Landing Page in JSON"),
            Link(href=f"{request.url}docs", rel="service-desc", title="API description in HTML"),
            Link(href=f"{request.url}openapi.json", rel="service-desc", title="API description in JSON"),
            # Link(href=f"{request.url}conformance", rel="data", title="Conformance Declaration in JSON"),
            Link(href=f"{request.url}collections", rel="data", title="Collections metadata in JSON"),
        ],
    )


async def get_collection_metadata(base_url: str, is_self) -> Collection:
    ts_request = dstore.GetTSAGRequest(attrs=["parameter_name", "standard_name", "unit", "level", "period", "function"])
    ts_response = await get_ts_ag_request(ts_request)
    # logger.info(ts_response.ByteSize())
    # logger.info(len(ts_response.groups))

    # Sadly, this is a different parameter as in the /locations endpoint, due to an error in the EDR spec
    # See: https://github.com/opengeospatial/ogcapi-environmental-data-retrieval/issues/427
    all_parameters: Dict[str, Parameter] = {}

    for group in ts_response.groups:
        ts = group.combo
        parameter = Parameter(
            description=f"{ts.standard_name} at {ts.level}m {ts.period} {ts.function}",
            observedProperty=ObservedProperty(
                id=f"https://vocab.nerc.ac.uk/standard_name/{ts.standard_name}",
                label=ts.parameter_name,
            ),
            unit=Unit(label=ts.unit),
        )
        if ts.parameter_name in all_parameters:
            raise HTTPException(
                status_code=500,
                detail={
                    "parameter": f"Parameter with name {ts.parameter_name} "
                    f"has multiple definitions:\n{all_parameters[ts.parameter_name]}\n{parameter}"
                },
            )

        all_parameters[ts.parameter_name] = parameter

    collection = Collection(
        id="observations",
        links=[
            Link(href=f"{base_url}/observations", rel="self" if is_self else "data"),
        ],
        extent=Extent(spatial=Spatial(bbox=[[3.0, 50.0, 8.0, 55.0]], crs="WGS84")),  # TODO: Get this from database
        data_queries=DataQueries(
            position=EDRQuery(
                link=EDRQueryLink(
                    href=f"{base_url}/observations/position",
                    rel="data",
                    variables=Variables(query_type="position", output_format=["CoverageJSON"]),
                )
            ),
            locations=EDRQuery(
                link=EDRQueryLink(
                    href=f"{base_url}/observations/locations",
                    rel="data",
                    variables=Variables(query_type="locations", output_format=["CoverageJSON"]),
                )
            ),
            area=EDRQuery(
                link=EDRQueryLink(
                    href=f"{base_url}/observations/area",
                    rel="data",
                    variables=Variables(query_type="area", output_format=["CoverageJSON"]),
                )
            ),
        ),
        crs=["WGS84"],
        output_formats=["CoverageJSON"],
        parameter_names={parameter_id: all_parameters[parameter_id] for parameter_id in sorted(all_parameters)},
    )
    return collection


async def get_collections(base_url: str) -> Collections:
    return Collections(
        links=[
            Link(href=f"{base_url}", rel="self"),
        ],
        collections=[await get_collection_metadata(base_url, is_self=False)],
    )
