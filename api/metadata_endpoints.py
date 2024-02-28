from datetime import datetime
from typing import Dict

from formatters.covjson import make_parameter
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
from edr_pydantic.parameter import Parameter, Parameters
from edr_pydantic.unit import Unit
from edr_pydantic.variables import Variables
from fastapi import HTTPException
from google.protobuf.timestamp_pb2 import Timestamp
from grpc_getter import get_obs_request


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


async def get_collection_metadata(request, is_self) -> Collection:
    # TODO: Try to remove/lower duplication with /locations endpoint
    ts_request = dstore.GetObsRequest(temporal_mode="latest")
    ts_response = await get_obs_request(ts_request)

    # Sadly, this is a different parameter as in the /locations endpoint, due to an error in the EDR spec
    # See: https://github.com/opengeospatial/ogcapi-environmental-data-retrieval/issues/427
    all_parameters: Dict[str, Parameter] = {}

    for obs in ts_response.observations:
        parameter = Parameter(
            description=obs.ts_mdata.title,
            observedProperty=ObservedProperty(
                id=f"https://vocab.nerc.ac.uk/standard_name/{obs.ts_mdata.standard_name}",
                label=obs.ts_mdata.parameter_name,
            ),
            unit=Unit(label=obs.ts_mdata.unit),
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

    collection = Collection(
        id="observations",
        links=[
            Link(href=f"{request.url}/observations", rel="self" if is_self else "data"),
        ],
        extent=Extent(spatial=Spatial(bbox=[[3.0, 50.0, 8.0, 55.0]], crs="WGS84")),  # TODO: Get this from database
        data_queries=DataQueries(
            position=EDRQuery(
                link=EDRQueryLink(
                    href=f"{request.url}/observations/position",
                    rel="data",
                    variables=Variables(query_type="position", output_format=["CoverageJSON"]),
                )
            ),
            locations=EDRQuery(
                link=EDRQueryLink(
                    href=f"{request.url}/observations/locations",
                    rel="data",
                    variables=Variables(query_type="locations", output_format=["CoverageJSON"]),
                )
            ),
            area=EDRQuery(
                link=EDRQueryLink(
                    href=f"{request.url}/observations/area",
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


async def get_collections(request) -> Collections:
    return Collections(
        links=[
            Link(href=f"{request.url}", rel="self"),
        ],
        collections=[await get_collection_metadata(request, is_self=False)],
    )
