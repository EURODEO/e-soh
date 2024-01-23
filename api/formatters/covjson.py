import math
from datetime import timezone
from itertools import groupby

from covjson_pydantic.coverage import Coverage
from covjson_pydantic.coverage import CoverageCollection
from covjson_pydantic.domain import Axes
from covjson_pydantic.domain import Domain
from covjson_pydantic.domain import DomainType
from covjson_pydantic.domain import ValuesAxis
from covjson_pydantic.ndarray import NdArray
from covjson_pydantic.observed_property import ObservedProperty
from covjson_pydantic.parameter import Parameter
from covjson_pydantic.reference_system import ReferenceSystem
from covjson_pydantic.reference_system import ReferenceSystemConnectionObject
from covjson_pydantic.unit import Unit
from fastapi import HTTPException
from formatters.base_formatter import EDR_formatter
from pydantic import AwareDatetime

# Requierd for pugin discovery
# Need to be available at top level of formatter plugin
formatter_name = "Covjson"


class Covjson(EDR_formatter):
    """
    Class for converting protobuf object to coverage json
    """

    def __init__(self):
        self.alias = ["covjson", "coveragejson"]
        self.mime_type = "application/json"  # find the type for covjson

    def convert(self, response):
        # Collect data
        coverages = []
        data = [self._collect_data(md.ts_mdata, md.obs_mdata) for md in response.observations]

        # Need to sort before using groupBy. Also sort on param_id to get consistently sorted output
        data.sort(key=lambda x: (x[0], x[1]))
        # The multiple coverage logic is not needed for this endpoint,
        # but we want to share this code between endpoints
        for (lat, lon, times), group in groupby(data, lambda x: x[0]):
            referencing = [
                ReferenceSystemConnectionObject(
                    coordinates=["y", "x"],
                    system=ReferenceSystem(type="GeographicCRS",
                                           id="http://www.opengis.net/def/crs/EPSG/0/4326"),
                ),
                ReferenceSystemConnectionObject(
                    coordinates=["z"],
                    system=ReferenceSystem(type="TemporalRS", calendar="Gregorian"),
                ),
            ]
            domain = Domain(
                domainType=DomainType.point_series,
                axes=Axes(
                    x=ValuesAxis[float](values=[lon]),
                    y=ValuesAxis[float](values=[lat]),
                    t=ValuesAxis[AwareDatetime](values=times),
                ),
                referencing=referencing,
            )

            parameters = {}
            ranges = {}
            for (_, _, _), param_id, unit, values in group:
                if all(math.isnan(v) for v in values):
                    continue  # Drop ranges if completely nan.
                    # TODO: Drop the whole coverage if it becomes empty?
                values_no_nan = [v if not math.isnan(v) else None for v in values]
                # TODO: Improve this based on "standard name", etc.
                parameters[param_id] = Parameter(
                    observedProperty=ObservedProperty(label={"en": param_id}), unit=Unit(label={"en": unit})
                )  # TODO: Also fill symbol?
                ranges[param_id] = NdArray(
                    values=values_no_nan, axisNames=["t", "y", "x"], shape=[len(values_no_nan), 1, 1]
                )

            coverages.append(Coverage(domain=domain, parameters=parameters, ranges=ranges))

        if len(coverages) == 0:
            raise HTTPException(status_code=404, detail="No data found")
        elif len(coverages) == 1:
            return coverages[0]
        else:
            return CoverageCollection(
                coverages=coverages, parameters=coverages[0].parameters
            )  # HACK to take parameters from first one

    def _collect_data(self, ts_mdata, obs_mdata):
        lat = obs_mdata[0].geo_point.lat  # HACK: For now assume they all have the same position
        lon = obs_mdata[0].geo_point.lon
        tuples = (
            (o.obstime_instant.ToDatetime(tzinfo=timezone.utc), float(o.value)) for o in obs_mdata
        )  # HACK: str -> float
        (times, values) = zip(*tuples)
        param_id = ts_mdata.instrument
        unit = ts_mdata.unit

        return (lat, lon, times), param_id, unit, values
