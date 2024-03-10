import json
import os

from esoh.api.app import app
from fastapi.testclient import TestClient


client = TestClient(app)


def test_post_json():
    project_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    test_succes_file = os.path.join(project_directory, "test/test_data/test_payload.json")
    test_fail_file = os.path.join(project_directory, "test/test_data/test1.json")

    with open(test_succes_file, "r") as file:
        json_data = json.load(file)
    response = client.post("/json?input_type=json", json=json_data)
    assert response.status_code == 200
    assert response.content.decode("utf-8") == '{"status_message":"succesfully published","status_code":200}'
    with open(test_fail_file, "r") as file:
        json_data = json.load(file)
    response = client.post("/json?input_type=json", json=json_data)
    assert response.status_code == 422
