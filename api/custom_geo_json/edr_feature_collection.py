from datetime import datetime
from datetime import timezone
from typing import List

from edr_pydantic.link import Link
from edr_pydantic.parameter import EdrBaseModel
from edr_pydantic.parameter import Parameters
from geojson_pydantic import FeatureCollection
from pydantic import Field
from pydantic.types import AwareDatetime


class EDRFeatureCollection(EdrBaseModel, FeatureCollection):
    parameters: Parameters
    # links: List[Link]
    timeStamp: AwareDatetime = Field(default=datetime.now(timezone.utc))
    # numberMatched: int
    # numberReturned: int
