import random

from locust import HttpUser
from locust import task

parameters = [
    "wind_speed:10.0:mean:PT10M",
    "wind_speed:10.0:mean:PT10M",
    "relative_humidity:2.0:mean:PT1M",
    "air_temperature:2.0:mean:PT1M",
]
# fmt: off
stations = [
    "0-20000-0-06203", "0-20000-0-06204", "0-20000-0-06205", "0-20000-0-06207", "0-20000-0-06208", "0-20000-0-06211",
    "0-20000-0-06214", "0-20000-0-06215", "0-20000-0-06235", "0-20000-0-06239", "0-20000-0-06242", "0-20000-0-06251",
    "0-20000-0-06260", "0-20000-0-06269", "0-20000-0-06270", "0-20000-0-06275", "0-20000-0-06279", "0-20000-0-06280",
    "0-20000-0-06290", "0-20000-0-06310", "0-20000-0-06317", "0-20000-0-06319", "0-20000-0-06323", "0-20000-0-06330",
    "0-20000-0-06340", "0-20000-0-06344", "0-20000-0-06348", "0-20000-0-06350", "0-20000-0-06356", "0-20000-0-06370",
    "0-20000-0-06375", "0-20000-0-06380",
]
# fmt: on
headers = {"Accept-Encoding": "br"}


class WebsiteUser(HttpUser):
    @task
    def get_data_single_station_single_parameter(self):
        parameter = random.choice(parameters)
        station_id = random.choice(stations)
        response = self.client.get(
            f"/collections/observations/locations/{station_id}?parameter-name={parameter}",
            name=f"single station {parameter}",
            headers=headers,
        )
        if response.status_code != 200:
            print(station_id, parameter)

    @task
    def get_data_bbox_three_parameters(self):
        self.client.get(
            "/collections/observations/area?parameter-name=wind_speed:10.0:mean:PT10M,wind_speed:10.0:mean:PT10M,relative_humidity:2.0:mean:PT1M&"
            "coords=POLYGON((5.0 52.0,6.0 52.0,6.0 52.1,5.0 52.1,5.0 52.0))",
            name="bbox",
            headers=headers,
        )
