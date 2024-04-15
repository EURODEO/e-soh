import json

import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture(scope="module")
def test_app():
    client = TestClient(app)
    yield client


def test_post_json_success(test_app, monkeypatch):
    with open("test/test_data/test_payload.json", "r") as file:
        json_data = json.load(file)
    monkeypatch.setattr("api.main.IngestToPipeline.ingest", lambda *a: ("Success", 200))

    response = test_app.post("/json?input_type=json", json=json_data)
    assert response.status_code == 200

    assert response.json() == {"status_message": "Successfully ingested", "status_code": 200}


def test_post_json_failure(test_app, monkeypatch):
    with open("test/test_data/test_payload.json", "r") as file:
        json_data = json.load(file)
    monkeypatch.setattr("api.main.IngestToPipeline.ingest", lambda *a: ("Success", 200))

    response = test_app.post("/json?input_type=json", json=json_data)
    assert response.status_code == 200
    assert response.json() == {"status_message": "Successfully ingested", "status_code": 200}
