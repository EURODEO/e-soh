#!/usr/bin/env python3
# tested with Python 3.11
import concurrent
import os
import uuid
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path
from time import perf_counter
from typing import List
from typing import Tuple

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc
import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp


def csv_file_to_requests(file_path: Path | str) -> Tuple[List, List]:
    time_format = "%Y%m%d %H:%M:%S"
    observation_request_messages = []
    ts_mdata = None
    obs_mdata = None

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path, encoding="iso-8859-15", encoding_errors="replace")
    df.dropna(subset=["DATA_VALUE"], inplace=True)
    df = df.drop_duplicates(subset=["STATION_ID", "MEASURAND_CODE", "DATA_TIME"])
    df.fillna("None", inplace=True)
    df = df.groupby("STATION_ID")
    for i, r in df:
        observations = []
        for i, r in r.iterrows():
            ts_mdata = dstore.TSMetadata(
                platform=str(r["STATION_ID"]),
                instrument=str(r["MEASURAND_CODE"]),
                title="FMI test data",
                standard_name=str(r["MEASURAND_CODE"]),
                unit=r["MEASURAND_UNIT"],
            )

            ts = Timestamp()

            ts.FromDatetime(datetime.strptime(r["DATA_TIME"], time_format))

            obs_mdata = dstore.ObsMetadata(
                id=str(uuid.uuid4()),
                geo_point=dstore.Point(lat=r["LATITUDE"], lon=r["LONGITUDE"]),
                obstime_instant=ts,
                value=str(r["DATA_VALUE"]),  # TODO: Store float in DB
            )

            observations.append(dstore.Metadata1(ts_mdata=ts_mdata, obs_mdata=obs_mdata))

        observation_request_messages.append(dstore.PutObsRequest(observations=observations))

    return observation_request_messages


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
    file_path = Path(Path(__file__).parents[2] / "test-data" / "FMI" / "20221231.csv")
    print(file_path)
    observation_request_messages = csv_file_to_requests(file_path=file_path)
    print("Finished creating the time series and observation requests " f"{perf_counter() - create_requests_start}.")

    insert_data(
        observation_request_messages=observation_request_messages,
    )

    print(f"Finished, total time elapsed: {perf_counter() - total_time_start}")
