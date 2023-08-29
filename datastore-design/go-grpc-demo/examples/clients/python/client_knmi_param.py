#!/usr/bin/env python3
# tested with Python 3.11
import concurrent
import os
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
from parameters import knmi_parameter_names
from google.protobuf.timestamp_pb2 import Timestamp


def netcdf_file_to_requests(file_path: Path | str) -> Tuple[List, List]:
    time_series_request_messages = []
    observation_request_messages = []
    # TODO: How to deal with IDs. At the moment, I set them manually, but if the database or server could handle it,
    #   it would help when going for parallel processing when inserting. Do we want to use a UUID?
    ts_id = 1

    with xr.open_dataset(file_path, engine="netcdf4", chunks=None) as file:  # chunks=None to disable dask
        for param_id in knmi_parameter_names:
            ts_observations = []

            param_file = file[param_id]
            for station_id, latitude, longitude, height in zip(
                file["station"].values, file["lat"].values[0], file["lon"].values[0], file["height"].values[0]
            ):
                tsMData = dstore.TSMetadata(
                    station_id=station_id,
                    param_id=param_id,
                    lat=latitude,
                    lon=longitude,
                    other1=param_file.name,
                    other2=param_file.long_name,
                    other3="value3",
                )
                request = dstore.AddTSRequest(
                    id=ts_id,
                    metadata=tsMData,
                )

                time_series_request_messages.append(request)

                station_slice = param_file.sel(station=station_id)

                observations = []
                for time, obs_value in zip(
                    pd.to_datetime(station_slice["time"].data).to_pydatetime(), station_slice.data
                ):
                    ts = Timestamp()
                    ts.FromDatetime(time)
                    observations.append(
                        dstore.Observation(
                            time=ts,
                            value=obs_value,
                            metadata=dstore.ObsMetadata(field1="KNMI", field2="Royal Dutch Meteorological Institute"),
                        )
                    )

                ts_observations.append(dstore.TSObservations(tsid=ts_id, obs=observations))
                ts_id += 1

            request = dstore.PutObsRequest(tsobs=ts_observations)
            observation_request_messages.append(request)

    return time_series_request_messages, observation_request_messages


def insert_data(time_series_request_messages: List, observation_request_messages: List):
    workers = int(cpu_count())

    with grpc.insecure_channel(f"{os.getenv('DSHOST', 'localhost')}:{os.getenv('DSPORT', '50050')}") as channel:
        client = dstore_grpc.DatastoreStub(channel=channel)

        print(f"Inserting {len(time_series_request_messages)} time series requests.")
        time_series_insert_start = perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            executor.map(client.AddTimeSeries, time_series_request_messages)
        print(f"Finished time series insert {perf_counter() - time_series_insert_start}.")

        print(f"Inserting {len(observation_request_messages)} bulk observations requests.")
        obs_insert_start = perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            executor.map(client.PutObservations, observation_request_messages)
        print(f"Finished observations bulk insert {perf_counter() - obs_insert_start}.")


if __name__ == "__main__":
    total_time_start = perf_counter()

    print("Starting with creating the time series and observations requests.")
    create_requests_start = perf_counter()
    file_path = Path(Path(__file__).parents[2] / "test-data" / "KNMI" / "20221231.nc")
    time_series_request_messages, observation_request_messages = netcdf_file_to_requests(file_path=file_path)
    print(f"Finished creating the time series and observation requests {perf_counter() - create_requests_start}.")

    insert_data(
        time_series_request_messages=time_series_request_messages,
        observation_request_messages=observation_request_messages,
    )

    print(f"Finished, total time elapsed: {perf_counter() - total_time_start}")
