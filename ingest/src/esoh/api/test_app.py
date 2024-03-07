import json

from fastapi.testclient import TestClient

from .app import app


client = TestClient(app)


def test_post_json():
    test_file = "/home/shamlym/workspace/e-soh/e-soh/ingest/test/test_data/test_payload.json"
    # Assuming you have a JSON file named 'data.json' containing your JSON data
    with open(test_file, "r") as file:
        json_data = json.load(file)
    response = client.post("/json", json=json_data)
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello World"}
