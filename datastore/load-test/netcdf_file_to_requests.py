import math
import uuid
from datetime import timedelta
from pathlib import Path
from time import perf_counter
from typing import List
from typing import Tuple

import datastore_pb2 as dstore
import pandas as pd
import xarray as xr
from google.protobuf.timestamp_pb2 import Timestamp


knmi_parameter_names = (
    "hc3",
    "nc2",
    "zm",
    "R1H",
    "hc",
    "tgn",
    "Tn12",
    "pr",
    "pg",
    "tn",
    "rg",
    "hc1",
    "nc1",
    "ts1",
    "nc3",
    "ts2",
    "qg",
    "ff",
    "ww",
    "gff",
    "dd",
    "td",
    "ww-10",
    "Tgn12",
    "ss",
    "Tn6",
    "dr",
    "rh",
    "hc2",
    "Tgn6",
    "R12H",
    "R24H",
    "Tx6",
    "Tx24",
    "Tx12",
    "Tgn14",
    "D1H",
    "R6H",
    "pwc",
    "tx",
    "nc",
    "pp",
    "Tn14",
    "ta",
)


def timerange(start_time, end_time, interval_minutes):
    current_time = start_time
    while current_time < end_time:
        yield current_time
        current_time += timedelta(minutes=interval_minutes)


def generate_dummy_requests_from_netcdf_per_station_per_timestamp(file_path: Path | str) -> Tuple[List, List]:
    print("Starting with creating the time series and observations requests.")
    create_requests_start = perf_counter()
    obs_per_station = []

    with xr.open_dataset(file_path, engine="netcdf4", chunks=None) as file:  # chunks=None to disable dask
        for station_id, latitude, longitude, height in zip(
            file["station"].values,
            file["lat"].values[0],
            file["lon"].values[0],
            file["height"].values[0],
        ):
            station_slice = file.sel(station=station_id)
            obs_per_timestamp = []
            for idx, time in enumerate(pd.to_datetime(station_slice["time"].data).to_pydatetime()):
                # Generate 100-sec data from each 10-min observation
                for i in range(0, 600, 100):  # 100-sec data
                    obs_per_parameter = []
                    generated_timestamp = time + timedelta(seconds=i)
                    ts = Timestamp()
                    ts.FromDatetime(generated_timestamp)
                    for param_id in knmi_parameter_names:
                        param = station_slice[param_id]
                        obs_value = station_slice[param_id].data[idx]  # Use 10 minute data value for each
                        obs_value = 0 if math.isnan(obs_value) else obs_value  # dummy data so obs_value doesn't matter
                        ts_mdata = dstore.TSMetadata(
                            platform=station_id,
                            instrument=param_id,
                            title=param.long_name,
                            standard_name=param.standard_name if "standard_name" in param.attrs else None,
                            unit=param.units if "units" in param.attrs else None,
                        )
                        obs_mdata = dstore.ObsMetadata(
                            id=str(uuid.uuid4()),
                            geo_point=dstore.Point(lat=latitude, lon=longitude),
                            obstime_instant=ts,
                            value=str(obs_value),
                        )
                        observation = dstore.Metadata1(ts_mdata=ts_mdata, obs_mdata=obs_mdata)
                        obs_per_parameter.append(observation)
                    obs_per_timestamp.append({"time": generated_timestamp, "observations": obs_per_parameter})
            obs_per_station.append(obs_per_timestamp)

    print("Finished creating the time series and observation requests " f"{perf_counter() - create_requests_start}.")
    print(f"Total number of obs generated per station is {len(obs_per_parameter) * len(obs_per_timestamp)}")
    return obs_per_station
