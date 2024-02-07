import json
from unittest.mock import patch

import datastore_pb2 as dstore
from fastapi.testclient import TestClient
from google.protobuf.json_format import Parse
from main import app


client = TestClient(app)


def test_get_locations_id_with_single_parameter_query_without_format():
    with patch("routers.edr.getObsRequest") as mock_getObsRequest:
        test_data = load_json("test/test_data/test_single_proto.json")
        compare_data = load_json("test/test_data/test_single_covjson.json")

        mock_getObsRequest.return_value = create_mock_obs_response(test_data)

        response = client.get(
            "/collections/observations/locations/06260?"
            + "parameter-name=ff&datetime=2022-12-31T00:00:00Z/2022-12-31T01:00:00Z"
        )

        # Check that getObsRequest gets called with correct arguments given in query
        mock_getObsRequest.assert_called_once()
        assert "ff" in mock_getObsRequest.call_args[0][0].filter["instrument"].values
        assert "06260" in mock_getObsRequest.call_args[0][0].filter["platform"].values

        assert response.status_code == 200
        assert response.json()["type"] == "Coverage"
        assert response.json() == compare_data


def test_get_locations_id_without_parameter_names_query():
    with patch("routers.edr.getObsRequest") as mock_getObsRequest:
        test_data = load_json("test/test_data/test_multiple_proto.json")
        compare_data = load_json("test/test_data/test_multiple_covjson.json")

        mock_getObsRequest.return_value = create_mock_obs_response(test_data)

        response = client.get("/collections/observations/locations/06260?f=covjson")

        # Check that getObsRequest gets called with correct arguments when no parameter names are given
        # in query
        mock_getObsRequest.assert_called_once()
        assert "*" in mock_getObsRequest.call_args[0][0].filter["instrument"].values

        assert response.status_code == 200
        assert response.json() == compare_data


def test_get_locations_id_with_incorrect_datetime_format():
    response = client.get("/collections/observations/locations/06260?datetime=20221231T000000Z/20221231T010000Z")

    assert response.status_code == 422
    assert response.json() == {"detail": {"datetime": "Invalid format: 20221231T000000Z/20221231T010000Z"}}


def test_get_locations_id_with_incorrect_datetime_range():
    response = client.get(
        "/collections/observations/locations/06260?datetime=2024-12-31T00:00:00Z/2022-12-31T01:00:00Z"
    )

    assert response.status_code == 422
    assert response.json() == {"detail": {"datetime": "Invalid range: 2024-12-31T00:00:00Z > 2022-12-31T01:00:00Z"}}


def create_mock_obs_response(json_data):
    response = dstore.GetObsResponse()
    Parse(json.dumps(json_data), response)
    return response


def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)
