# tested with Python 3.11
import math
import re
from pathlib import Path
from time import perf_counter

import pandas as pd
import requests
import xarray as xr
from parameters import knmi_parameter_names


regex_level = re.compile(r"[0-9]+(\.[0-9]+)?(?=m)|(?<=Level )[0-9]+", re.IGNORECASE)
regex_level_centimeters = re.compile(r"[0-9]+(\.[0-9]+)?(?=cm)")
regex_time_period = re.compile(r"([0-9]+) (Hours|Min)", re.IGNORECASE)
regex_time_period_without_number = re.compile(r"[^0-9]+? (Hour|Min)", re.IGNORECASE)


def netcdf_file_to_requests(file_path: Path | str):
    features = []

    with xr.open_dataset(file_path, engine="netcdf4", chunks=None) as file:  # chunks=None to disable dask
        for station_id, station_name, latitude, longitude, height in zip(
            file["station"].values,
            file["stationname"].values[0],
            file["lat"].values[0],
            file["lon"].values[0],
            file["height"].values[0],
        ):
            station_slice = file.sel(station=station_id)

            for param_id in knmi_parameter_names:
                # if param_id in ["hc1", "hc2", "hc3", "nc1", "nc2", "nc3"]:  # TODO!!!
                #     continue
                # print(station_id, param_id)
                param_file = station_slice[param_id]
                if "standard_name" not in param_file.attrs:
                    print(param_file.long_name)
                    continue

                # if param_file.standard_name in [
                #     "precipitation_duration",
                #     "precipitation_rate",
                #     "rainfall_duration",
                #     "total_downwelling_shortwave_flux_in_air",
                # ]:
                #     continue

                standard_name, level, function, period = generate_parameter_name(
                    param_file.standard_name,
                    param_file.long_name,
                    station_id,
                    station_name,
                    param_id,
                )
                platform = f"0-20000-0-{station_id}"

                base_content = {
                    "encoding": "utf-8",
                    "standard_name": standard_name,
                    "unit": param_file.units if "units" in param_file.attrs else None,
                }

                base_properties = {
                    "platform": platform,
                    "instrument": param_id,
                    "title": param_file.long_name,
                    "summary": "KNMI collects observations from the automatic weather stations situated in "
                    "the Netherlands and BES islands on locations such as aerodromes and "
                    "North Sea platforms. In addition, wind data from KNMI wind poles are included. "
                    "The weather stations report every 10 minutes meteorological parameters "
                    "such as temperature, relative humidity, wind, air pressure, visibility, precipitation, "
                    "and cloud cover. The number of parameters differs per station. The file for the past "
                    "10 minutes is available a few minutes later and contains a timestamp denoting the end "
                    "of the observation period in UTC. It is possible that a station's observations may not "
                    "be immediately available.",
                    "level": level,
                    "period": period,
                    "function": function,
                    "naming_authority": "nl.knmi",
                    "keywords": file["iso_dataset"].attrs["keyword"],
                    "keywords_vocabulary": file.attrs["references"],
                    "creator_name": "Royal Netherlands Meteorological Institute (KNMI)",
                    "creator_email": "opendata@knmi.nl",  # file["iso_dataset"].attrs["email_dataset"],
                    "creator_url": "https://dataplatform.knmi.nl/",  # file["iso_dataset"].attrs["url_metadata"],
                    "creator_type": "institution",
                    "institution": file.attrs["institution"],
                    "license": "https://creativecommons.org/licenses/by/4.0/",
                    "Conventions": station_slice.Conventions,
                }

                for time, obs_value in zip(
                    pd.to_datetime(param_file["time"].data).to_pydatetime(),
                    param_file.data,
                ):
                    if not math.isnan(obs_value):
                        content = base_content.copy()
                        properties = base_properties.copy()

                        properties["datetime"] = time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")  # TODO: Format is unexpected
                        content["size"] = param_file.nbytes  # TODO: Take bytes of entire file or slice?
                        content["value"] = str(obs_value)
                        properties["content"] = content

                        feature = {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": {"lat": latitude, "lon": longitude}},
                            "properties": properties,
                            "links": [
                                {
                                    "href": "https://dataplatform.knmi.nl/",
                                    "rel": "describedby",
                                    "type": "text/html",
                                    "hreflang": "en",
                                    "title": "KNMI Data Platform",
                                },
                                {
                                    "href": "https://developer.dataplatform.knmi.nl/",
                                    "rel": "describedby",
                                    "type": "text/html",
                                    "hreflang": "en",
                                    "title": "KNMI Developer Portal",
                                },
                            ],
                            "version": "1.0",
                        }
                        features.append(feature)

    return features


def generate_parameter_name(standard_name, long_name, station_id, station_name, param_id):
    # TODO: HACK To let the loader have a unique parameter ID and make the parameters distinguishable.
    level = "2.0"
    long_name = long_name.lower()
    station_name = station_name.lower()

    if standard_name == "air_pressure_at_sea_level":
        standard_name = "air_pressure_at_mean_sea_level"

    if level_raw := re.search(regex_level_centimeters, long_name):
        level = str(float(level_raw[0]) / 100.0)
    elif level_raw := re.search(regex_level, long_name):
        level = level_raw[0]
    elif "grass" in long_name:
        level = "0"
    elif param_id in ["pg", "pr", "pwc", "vv", "W10", "W10-10", "ww", "ww-10", "za", "zm"]:
        # https://english.knmidata.nl/open-data/actuele10mindataknmistations
        # Comments code: 2, 3, 11
        # Note: The sensor is not installed at equal heights at all types of measurement sites:
        # At 'AWS' sites the device is installed at 1.80m. At 'AWS/Aerodrome' and 'Mistpost'
        # (note that this includes site Voorschoten (06215) which is 'AWS/Mistpost')
        # the device is installed at 2.50m elevation. Exceptions are Berkhout AWS (06249),
        # De Bilt AWS (06260) and Twenthe AWS (06290) where the sensor is installed at 2.50m.
        # Since WaWa is automatic detection I asssumed that the others stations are AWS, thus 1.80m
        if (
            station_id in ["06215", "06249", "06260", "06290"]
            or "aerodrome" in station_name
            or "mistpost" in station_name
        ):
            level = "2.50"
        else:
            level = "1.80"

    if "minimum" in long_name:
        function = "minimum"
    elif "maximum" in long_name:
        function = "maximum"
    elif "average" in long_name:
        function = "mean"
    else:
        function = "point"

    period = "PT0S"
    if period_raw := re.findall(regex_time_period, long_name):
        if len(period_raw) == 1:
            period_raw = period_raw[0]
        else:
            raise Exception(f"{period_raw}, {long_name}")
        time, scale = period_raw
        match scale:
            case "hours":
                period = f"PT{time}H"
            case "min":
                period = f"PT{time}M"
    elif period_raw := re.search(regex_time_period_without_number, long_name):
        match period_raw[1]:
            case "hour":
                period = "PT01H"
            case "min":
                period = "PT01M"
    elif param_id == "ww-10":
        period = "PT10M"
    elif param_id == "ww":
        period = "PT01H"

    return standard_name, level, function, period


if __name__ == "__main__":
    total_time_start = perf_counter()

    print("Starting with creating the time series and observations requests.")
    create_requests_start = perf_counter()
    file_path = Path(Path(__file__).parent / "test-data" / "KNMI" / "20221231.nc")
    features = netcdf_file_to_requests(file_path=file_path)
    print("Finished creating the features " f"{perf_counter() - create_requests_start}.")
    total_features = len(features)
    print(total_features)

    value_errors = {}

    for i, f in enumerate(features):
        response = requests.post("http://localhost:8009/json", json=f)
        print(f"Feature {i} out of {total_features}.")
        if response.status_code != 200:
            try:
                value_errors[f["properties"]["instrument"]] = {
                    "parameter_name": ":".join(
                        [
                            f["properties"]["content"]["standard_name"],
                            f["properties"]["level"],
                            f["properties"]["function"],
                            f["properties"]["period"],
                        ]
                    ),
                    "title": f["properties"]["title"],
                    "value_error": response.json()["detail"][0]["msg"],
                }
                print(f"Inbetween overview value errors:\n{value_errors}")
            except Exception:
                print(response.status_code)
                print(response.content)

    print(f"All value errors:\n{value_errors}")
    print(f"Finished, total time elapsed: {perf_counter() - total_time_start}")
