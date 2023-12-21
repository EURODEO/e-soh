from pathlib import Path

import datastore_pb2 as dstore
import datastore_pb2_grpc as dstore_grpc
import grpc_user
import psycopg2
from locust import between
from locust import events
from locust import task
from netcdf_file_to_requests import generate_dummy_requests_from_netcdf_per_station_per_timestamp


file_path = Path(Path(__file__).parents[1] / "test-data" / "KNMI" / "20230101.nc")

stations = [
    "06201",
    "06203",
    "06204",
    "06205",
    "06207",
    "06208",
    "06211",
    "06214",
    "06215",
    "06225",
    "06229",
    "06235",
    "06239",
    "06240",
    "06242",
    "06248",
    "06249",
    "06251",
    "06252",
    "06257",
    "06258",
    "06260",
    "06267",
    "06269",
    "06270",
    "06273",
    "06275",
    "06277",
    "06278",
    "06279",
    "06280",
    "06283",
    "06286",
    "06290",
    "06310",
    "06317",
    "06319",
    "06320",
    "06321",
    "06323",
    "06330",
    "06340",
    "06343",
    "06344",
    "06348",
    "06350",
    "06356",
    "06370",
    "06375",
    "06377",
    "06380",
    "06391",
    "78871",
    "78873",
    "78990",
]


class IngestionGrpcUser(grpc_user.GrpcUser):
    host = "localhost:50050"
    stub_class = dstore_grpc.DatastoreStub
    wait_time = between(1.5, 2.5)
    user_nr = 0
    dummy_observations_all_stations = generate_dummy_requests_from_netcdf_per_station_per_timestamp(file_path)
    weight = 7

    def on_start(self):
        print(f"User {IngestionGrpcUser.user_nr}")
        self.dummy_observations_per_station = IngestionGrpcUser.dummy_observations_all_stations[
            IngestionGrpcUser.user_nr
        ]
        IngestionGrpcUser.user_nr += 1
        self.index = 0

    @task
    def ingest_data_per_observation(self):
        # 44 observations per task
        observations = self.dummy_observations_per_station[self.index]["observations"]
        request_messages = dstore.PutObsRequest(observations=observations)
        response = self.stub.PutObservations(request_messages)
        assert response.status == -1
        self.index += 1

    @events.test_stop.add_listener
    def on_test_stop(environment, **kwargs):
        print("Cleaning up test data")
        conn = psycopg2.connect(
            database="data", user="postgres", password="mysecretpassword", host="localhost", port="5433"
        )
        cursor = conn.cursor()
        # delete all details from observations table for date 20230101
        sql = """ DELETE FROM observation WHERE extract(YEAR from obstime_instant)::int = 2023 """
        cursor.execute(sql)
        # Commit your changes in the database
        conn.commit()
        conn.close()
