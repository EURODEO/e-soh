from fastapi import HTTPException
from geojson_pydantic import Feature
from geojson_pydantic import FeatureCollection
from geojson_pydantic import Point


def _make_properties(ts):
    ts_metadata = {key.name: value for key, value in ts.ts_mdata.ListFields() if value}

    ts_metadata["platform_vocabulary"] = (
        "https://oscar.wmo.int/surface/#/search/station/stationReportDetails/" + ts.ts_mdata.platform
        if not ts.ts_mdata.platform_vocabulary
        else ts.ts_mdata.platform_vocabulary
    )
    # This should also be compatible with future when name is added to datastore
    if "name" not in ts_metadata:
        ts_metadata["name"] = ts.ts_mdata.platform  # TODO: grab proper name when implemented in proto

    return ts_metadata


def convert_to_geojson(response):
    """
    Will only generate geoJSON for stationary timeseries
    """
    features = [
        Feature(
            type="Feature",
            id=ts.obs_mdata[0].metadata_id,
            properties=_make_properties(ts=ts),
            geometry=Point(
                type="Point",
                coordinates=(
                    ts.obs_mdata[0].geo_point.lon,
                    ts.obs_mdata[0].geo_point.lat,
                ),
            ),
        )
        for ts in sorted(response.observations, key=lambda ts: ts.obs_mdata[0].metadata_id)
    ]
    if not features:
        raise HTTPException(404, detail="Query did not return any time series.")
    return FeatureCollection(features=features, type="FeatureCollection") if len(features) > 1 else features[0]
