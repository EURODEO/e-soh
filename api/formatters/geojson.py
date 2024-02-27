from geojson_pydantic import Feature
from geojson_pydantic import FeatureCollection
from geojson_pydantic import Point


def _make_properties(ts):
    return {
        "version": ts.ts_mdata.version,
        "type": ts.ts_mdata.type,
        "summary": ts.ts_mdata.summary,
        "keywords": ts.ts_mdata.keywords,
        "keywords_vocabulary": ts.ts_mdata.keywords_vocabulary,
        "conventions": ts.ts_mdata.conventions,
        "naming_authority": ts.ts_mdata.naming_authority,
        "creator_type": ts.ts_mdata.creator_type,
        "creator_name": ts.ts_mdata.creator_name,
        "creator_email": ts.ts_mdata.creator_email,
        "creator_url": ts.ts_mdata.creator_url,
        "institution": ts.ts_mdata.institution,
        "project": ts.ts_mdata.project,
        "source": ts.ts_mdata.source,
        "platform": ts.ts_mdata.platform,
        "platform_vocabulary": ts.ts_mdata.platform_vocabulary,
        "standard_name": ts.ts_mdata.standard_name,
        "unit": ts.ts_mdata.unit,
        "instrument": ts.ts_mdata.instrument,
        "instrument_vocabulary": ts.ts_mdata.instrument_vocabulary,
        "level": ts.ts_mdata.level,
        "period": ts.ts_mdata.period,
        "function": ts.ts_mdata.function,
        "parameter_name": ts.ts_mdata.parameter_name,
        "history": ts.obs_mdata[0].history,
        "processing_level": ts.obs_mdata[0].processing_level,
    }


def convert_to_geojson(response):
    """
    Will only genereate geoJSON for stationary timeseries
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
        for ts in sorted(response.observations, key=lambda ts: ts.ts_mdata.platform)
    ]
    return FeatureCollection(features=features, type="FeatureCollection") if len(features) > 1 else features[0]
