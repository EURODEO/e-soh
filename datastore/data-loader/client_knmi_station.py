#!/usr/bin/env python3
# tested with Python 3.11
import concurrent
import math
import os
import re
import uuid
from multiprocessing import cpu_count
from pathlib import Path
from time import perf_counter
from typing import List
from typing import Tuple

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
import pandas as pd
import xarray as xr
from google.protobuf.timestamp_pb2 import Timestamp
from parameters import knmi_parameter_names

regex_level = re.compile(r"first|second|third|[0-9]+(\.[0-9]+)?(?=m)|(?<=Level )[0-9]+", re.IGNORECASE)
regex_level_centimeters = re.compile(r"[0-9]+(\.[0-9]+)?(?=cm)")
regex_time_period = re.compile(r"(\d+) (Hours|Min)", re.IGNORECASE)


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
                # print(station_id, param_id)
                param_file = station_slice[param_id]
                standard_name, level, function, period = generate_parameter_name(
                    (param_file.standard_name if "standard_name" in param_file.attrs else "placeholder"),
                    param_file.long_name,
                    station_id,
                    station_name,
                    param_id,
                )

                ts_mdata = dstore.TSMetadata(
                    platform=f"0-20000-0-{station_id}",
                    instrument=param_id,
                    title=param_file.long_name,
                    standard_name=standard_name,
                    unit=param_file.units if "units" in param_file.attrs else None,
                    level=level,
                    period=period,
                    function=function,
                    parameter_name=":".join([standard_name, level, function, period]),
                )

                for time, obs_value in zip(
                    pd.to_datetime(param_file["time"].data).to_pydatetime(),
                    param_file.data,
                ):
                    ts = Timestamp()
                    ts.FromDatetime(time)
                    if not math.isnan(obs_value):  # Stations that don't have a parameter give them all as nan
                        obs_mdata = dstore.ObsMetadata(
                            id=str(uuid.uuid4()),
                            geo_point=dstore.Point(lat=latitude, lon=longitude),
                            obstime_instant=ts,
                            value=str(obs_value),  # TODO: Store float in DB
                        )
                        observations.append(dstore.Metadata1(ts_mdata=ts_mdata, obs_mdata=obs_mdata))

            if len(observations) > 0:
                observation_request_messages.append(dstore.PutObsRequest(observations=observations))

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

    return standard_name, level, function, period


def insert_data(observation_request_messages: List):
    workers = int(cpu_count())

    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        client = dstore_grpc.DatastoreStub(channel=channel)

        print(f"Inserting {len(observation_request_messages)} bulk observations requests.")
        obs_insert_start = perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for _ in executor.map(client.PutObservations, observation_request_messages):
                pass
        print(f"Finished observations bulk insert {perf_counter() - obs_insert_start}.")


if __name__ == "__main__":
    total_time_start = perf_counter()

    print("Starting with creating the time series and observations requests.")
    create_requests_start = perf_counter()
    file_path = Path(Path(__file__).parent / "test-data" / "KNMI" / "20221231.nc")
    observation_request_messages = netcdf_file_to_requests(file_path=file_path)
    print("Finished creating the time series and observation requests " f"{perf_counter() - create_requests_start}.")

    insert_data(
        observation_request_messages=observation_request_messages,
    )

    print(f"Finished, total time elapsed: {perf_counter() - total_time_start}")
