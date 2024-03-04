from typing import Dict

from covjson_pydantic.parameter import Parameter
from edr_pydantic.parameter import EdrBaseModel
from geojson_pydantic import FeatureCollection


# TODO: In the future move this to https://github.com/KNMI/edr-pydantic
#  some work was already done in https://github.com/KNMI/edr-pydantic/pull/7
class EDRFeatureCollection(EdrBaseModel, FeatureCollection):
    parameters: Dict[str, Parameter]
