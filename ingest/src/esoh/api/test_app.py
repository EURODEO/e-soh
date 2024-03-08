import json

from esoh.api.app import app
from fastapi.testclient import TestClient


client = TestClient(app)


def test_post_json():
    test_file = "/home/shamlym/workspace/e-soh/e-soh/ingest/test/test_data/test_payload.json"
    with open(test_file, "r") as file:
        json_data = json.load(file)
    response = client.post("/json?input_type=json", json=json_data)
    assert response.status_code == 200
    assert (
        response.content.decode("utf-8")
        == '{"content":{"status":"success","detail":"Operation successful"},"status_code":200}'
    )
