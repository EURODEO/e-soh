import random

from locust import HttpUser
from locust import task

parameters = ["ff", "dd", "rh", "pp", "tn"]
# fmt: off
stations = [
    "06203", "06204", "06205", "06207", "06208", "06211", "06214", "06215", "06235", "06239",
    "06242", "06251", "06260", "06269", "06270", "06275", "06279", "06280", "06290", "06310",
    "06317", "06319", "06323", "06330", "06340", "06344", "06348", "06350", "06356", "06370",
    "06375", "06380", "78871", "78873",
]
# fmt: on
headers = {"Accept-Encoding": "br"}


class WebsiteUser(HttpUser):
    @task
    def get_data_single_station_single_parameter(self):
        parameter = random.choice(parameters)
        station_id = random.choice(stations)
        self.client.get(
            f"/collections/observations/locations/{station_id}?parameter-name={parameter}",
            name=f"single station {parameter}",
            headers=headers,
        )

    @task
    def get_data_bbox_three_parameters(self):
        self.client.get(
            "/collections/observations/area?parameter-name=dd,ff,rh&"
            "coords=POLYGON((5.0 52.0,6.0 52.0,6.0 52.1,5.0 52.1,5.0 52.0))",
            name="bbox",
            headers=headers,
        )
