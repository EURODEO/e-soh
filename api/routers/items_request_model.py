from typing import Annotated

import formatters
from fastapi import Query


class ItemsQueryParams:
    def __init__(
        self,
        bbox: Annotated[str | None, Query(example="5.0,52.0,6.0,52.1")] = None,
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
        self.bbox = bbox
        self.datetime = datetime
        self.ids = ids
        self.parameter_name = parameter_name
        self.naming_authority = naming_authority
        self.institution = institution
        self.platform = platform
        self.standard_name = standard_name
        self.unit = unit
        self.instrument = instrument
        self.level = level
        self.period = period
        self.f = f
