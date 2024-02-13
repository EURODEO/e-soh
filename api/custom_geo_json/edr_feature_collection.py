from typing import Dict

from covjson_pydantic.parameter import Parameter
from edr_pydantic.parameter import EdrBaseModel
from geojson_pydantic import FeatureCollection


class EDRFeatureCollection(EdrBaseModel, FeatureCollection):
    parameters: Dict[str, Parameter]
