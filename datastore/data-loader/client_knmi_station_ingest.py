#!/usr/bin/env python3
# tested with Python 3.11
import math
import os
import re
import requests
import json
from multiprocessing import cpu_count, Pool
from pathlib import Path
from time import perf_counter
from typing import List
from typing import Tuple

import pandas as pd
import xarray as xr
from google.protobuf.timestamp_pb2 import Timestamp
from parameters import knmi_parameter_names


regex_level = re.compile(r"first|second|third|[0-9]+(\.[0-9]+)?(?=m)|(?<=Level )[0-9]+", re.IGNORECASE)
regex_level_centimeters = re.compile(r"[0-9]+(\.[0-9]+)?(?=cm)")
regex_time_period = re.compile(r"(\d+) (Hours|Min)", re.IGNORECASE)


def convert_standard_names_to_cf(standard_name):
    standard_name_mapping = {
        "cloud_cover": "cloud_area_fraction",
        "total_downwelling_shortwave_flux_in_air": "surface_downwelling_shortwave_flux_in_air",
        "precipitation_rate": "rainfall_rate",
        "air_pressure_at_sea_level": "air_pressure_at_mean_sea_level",
    }
    return standard_name_mapping.get(standard_name, standard_name)


# NOTE: Only units are converted currently, not values.
def convert_unit_names(unit):
    unit_mapping = {
        "degrees Celsius": "degC",
        "ft": "m",
        "min": "s",
        "degree": "degrees",
        "%": "percent",
        "mm": "kg/m2",
        "m s-1": "m/s",
        "octa": "oktas",
        "W m-2": "W/m2",
    }
    return unit_mapping.get(unit, unit)


def netcdf_file_to_requests(file_path: Path | str) -> Tuple[List, List]:
    observation_request_messages = []

    with xr.open_dataset(file_path, engine="netcdf4", chunks=None) as file:  # chunks=None to disable dask
        for station_id, station_name, latitude, longitude, height in zip(
            file["station"].values,
            file["stationname"].values[0],
            file["lat"].values[0],
            file["lon"].values[0],
            file["height"].values[0],
        ):
            observations = []
            station_slice = file.sel(station=station_id)

            for param_id in knmi_parameter_names:
                param_file = station_slice[param_id]
                standard_name, level, function, period = generate_parameter_name(
                    (param_file.standard_name if "standard_name" in param_file.attrs else None),
                    param_file.long_name,
                    station_id,
                    station_name,
                    param_id,
                )

                if not standard_name or standard_name in ["precipitation_duration", "rainfall_duration"]:
                    continue

                platform = f"0-20000-0-{station_id}"

                properties = {
                    "platform": platform,
                    "instrument": param_id,
                    "platform_name": station_name,
                    "title": param_file.long_name,
                    "Conventions": "CF-1.8",
                    "license": "CC BY 4.0",
                    "level": level,
                    "period": period,
                    "function": function,
                    "parameter_name": ":".join([standard_name, level, function, period]),
                    "naming_authority": "nl.knmi",
                    "keywords": file["iso_dataset"].attrs["keyword"],
                    "summary": file["iso_dataset"].attrs["abstract"],
                    "keywords_vocabulary": file.attrs["references"],
                    "source": file.attrs["source"],
                    "creator_name": "KNMI",
                    "creator_email": file["iso_dataset"].attrs["email_dataset"],
                    "creator_url": file["iso_dataset"].attrs["url_metadata"],
                    "creator_type": "institution",
                    "institution": file.attrs["institution"],
                }

                geometry = {
                    "type": "Point",
                    "coordinates": {
                        "lon": longitude,
                        "lat": latitude,
                    },
                }

                for time, obs_value in zip(
                    pd.to_datetime(param_file["time"].data).to_pydatetime(),
                    param_file.data,
                ):

                    ts = Timestamp()
                    ts.FromDatetime(time)
                    if not math.isnan(obs_value):  # Stations that don't have a parameter give them all as nan
                        content = {
                            "encoding": "utf-8",
                            "standard_name": standard_name,
                            "unit": convert_unit_names(param_file.units),
                            "value": str(obs_value),
                        }

                        observations.append(
                            {
                                "properties": {
                                    **properties,
                                    "content": content,
                                    "datetime": ts.ToJsonString(),
                                },
                                "geometry": geometry,
                                "version": "4.0",
                                "type": "Feature",
                                "links": [
                                    {"href": "Insert documentation about E-SOH datastore", "rel": "canonical"},
                                ],
                            }
                        )

            if len(observations) > 0:
                observation_request_messages.append(observations)

    return observation_request_messages


def generate_parameter_name(standard_name, long_name, station_id, station_name, param_id):
    # TODO: HACK To let the loader have a unique parameter ID and make the parameters distinguishable.
    level = "2.0"
    long_name = long_name.lower()
    station_name = station_name.lower()
    if level_raw := re.search(regex_level, long_name):
        level = level_raw[0]
    if level_raw := re.search(regex_level_centimeters, long_name):
        level = str(float(level_raw[0]) / 100.0)
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

    # We want "level" to be numeric, so get rid of "first", "second" and "third".
    # Note that this level has no meaning, it is just a hack for this test dataset
    if level == "first":
        level = "0"
    if level == "second":
        level = "1"
    if level == "third":
        level = "2.0"

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
        if scale == "hours":
            period = f"PT{time}H"
        elif scale == "min":
            period = f"PT{time}M"
    elif param_id == "ww-10":
        period = "PT10M"
    elif param_id == "ww":
        period = "PT01H"

    standard_name = convert_standard_names_to_cf(standard_name)
    return standard_name, level, function, period


def send_request_to_ingest(msg, url):
    try:
        response = requests.post(url, data=json.dumps(msg))
        response.raise_for_status()
        return response.status_code, response.json()
    except requests.RequestException as e:
        return response.status_code, e


def insert_data(observation_request_messages: List, url):
    with Pool(cpu_count()) as pool:
        results = pool.starmap(send_request_to_ingest, [(msg, url) for msg in observation_request_messages])

        for status_code, response in results:
            if status_code != 200:
                print(status_code, response)
                return


def main(observation_request_messages: List, url):
    print(f"Sending {len(observation_request_messages)} bulk observations requests to ingest.")
    obs_insert_start = perf_counter()
    insert_data(observation_request_messages, url)
    print(f"Finished observations bulk insert {perf_counter() - obs_insert_start}.")


if __name__ == "__main__":
    total_time_start = perf_counter()
    print("Starting with creating the time series and observations requests.")
    create_requests_start = perf_counter()
    file_path = Path(Path(__file__).parent / "test-data" / "KNMI" / "20221231.nc")
    observation_request_messages = netcdf_file_to_requests(file_path=file_path)
    print("Finished creating the time series and observation requests " f"{perf_counter() - create_requests_start}.")

    main(
        observation_request_messages=observation_request_messages,
        url=os.getenv("INGEST_URL", "http://localhost:8009/json"),
    )

    print(f"Finished, total time elapsed: {perf_counter() - total_time_start}")
