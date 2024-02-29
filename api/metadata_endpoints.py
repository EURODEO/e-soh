# from dependencies import get_current_parameter_names
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
from edr_pydantic.variables import Variables


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


def get_collection_metadata(request) -> Collection:
    collection = Collection(
        id="observations",
        links=[
            Link(href=f"{request.url}/observations", rel="self"),
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
        parameter_names={},  # TODO: Get these from database
    )
    return collection


def get_collections(request) -> Collections:
    return Collections(
        links=[
            Link(href=f"{request.url}", rel="self"),
        ],
        collections=[get_collection_metadata(request)],
    )
