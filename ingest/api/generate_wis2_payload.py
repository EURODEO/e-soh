import os

from api.model import Geometry
from api.model import Link
from api.wis2_model import Wis2MessageSchema
from api.wis2_model import Properties
from api.wis2_model import Content


def get_api_timeseries_query(location_id: str, baseURL: str) -> str:
    query = "/collections/observations/locations/" + location_id
    baseURL = os.getenv("EDR_API_URL", baseURL)
    return baseURL + query


def generate_wis2_topic() -> str:
    """This function will generate the WIS2 complient toipc name"""
    wis2_topic = os.getenv("WIS2_TOPIC")
    return wis2_topic


def generate_wis2_payload(message: dict, request_url: str) -> Wis2MessageSchema:
    """
    This function will generate the WIS2 complient payload based on the JSON schema for ESOH
    """
    wis2_payload = Wis2MessageSchema(
        type="Feature",
        id=message["id"],
        version="v04",
        geometry=Geometry(**message["geometry"]),
        properties=Properties(
            producer=message["properties"]["naming_authority"],
            data_id=message["properties"]["data_id"],
            metadata_id="TOBESET",  # Need to figure out how we generate this? Is it staic or dynamic?
            datetime=message["properties"]["datetime"],
            pubtime=message["properties"]["pubtime"],
            content=Content(value="test", unit="test", encoding="utf-8", standard_name="test"),
            # content=Content() # Will leave content empty at the moment.
            # I think the content should be somewhat the same format as you get from the API.
            # Maybe it should be a small geojson feature object with complete data?
        ),
        links=(
            [
                Link(
                    href=get_api_timeseries_query(message["properties"]["platform"], request_url),
                    rel="canonical",
                    type="application/prs.coverage+json",
                )
            ]
        )
        + (lambda x: x if x else [])(message["links"]),
    )

    return wis2_payload
