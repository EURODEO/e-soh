import json
import logging
import os
from pathlib import Path

import requests
from deepdiff import DeepDiff

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


BASE_URL = os.environ.get("BASE_URL", "http://localhost:8008")


def actual_status_code_is_expected_status_code(actual_response, expected_status_code):
    assert actual_response.status_code == expected_status_code


def actual_response_is_expected_response(actual_response, expected_path):
    file_path = Path(Path(__file__).parent, expected_path).resolve()
    with open(file_path) as file:
        expected_json = json.load(file)

    diff = DeepDiff(expected_json, actual_response.json(), ignore_order=True)
    assert diff == {}


def test_get_all_collections():
    actual_response = requests.get(url=BASE_URL + "/collections")

    actual_status_code_is_expected_status_code(actual_response, 200)
    actual_response_is_expected_response(actual_response, "response/capabilities/200/all_collections.json")


def test_get_a_single_existing_collection():
    collection_id = "observations"
    actual_response = requests.get(url=BASE_URL + f"/collections/{collection_id}")

    actual_status_code_is_expected_status_code(actual_response, 200)
    actual_response_is_expected_response(actual_response, "response/metadata/200/single_collection.json")


def test_get_a_collection_which_does_not_exist():
    collection_id = "does-not-exist"
    actual_response = requests.get(url=BASE_URL + f"/collections/{collection_id}")

    actual_status_code_is_expected_status_code(actual_response, 404)
    actual_response_is_expected_response(actual_response, "response/metadata/404/not_found.json")


def test_from_a_single_collection_get_locations_within_a_bbox():
    collection_id = "observations"
    bbox = "5.0,52.0,6.0,52.1"
    actual_response = requests.get(url=BASE_URL + f"/collections/{collection_id}/locations?bbox={bbox}")

    actual_status_code_is_expected_status_code(actual_response, 200)
    actual_response_is_expected_response(
        actual_response, "response/collection/locations/200/locations_within_a_bbox.json"
    )


def test_from_a_single_collection_get_a_single_location():
    collection_id = "observations"
    location_id = "06260"
    parameters = "dd,ff,rh"
    datetime = "2022-12-31T00:50:00Z/2022-12-31T02:10:00Z"
    actual_response = requests.get(
        url=BASE_URL + f"/collections/{collection_id}/locations/{location_id}"
        f"?parameter-name={parameters}&datetime={datetime}"
    )

    actual_status_code_is_expected_status_code(actual_response, 200)
    actual_response_is_expected_response(
        actual_response, "response/collection/locations/200/single_location_with_multiple_parameters.json"
    )


def test_from_a_single_collection_get_a_single_location_which_does_not_exist():
    collection_id = "observations"
    location_id = "does-not-exist"
    parameters = "does-not-exist"
    actual_response = requests.get(
        url=BASE_URL + f"/collections/{collection_id}/locations/{location_id}?parameter-name={parameters}"
    )

    actual_status_code_is_expected_status_code(actual_response, 404)
    actual_response_is_expected_response(actual_response, "response/collection/locations/404/no_data_found.json")


def test_from_a_single_collection_get_a_single_position_with_multiple_parameters():
    collection_id = "observations"
    coords = "POINT(5.179705 52.0988218)"
    parameters = "dd,ff,rh"
    datetime = "2022-12-31T00:50:00Z/2022-12-31T02:10:00Z"
    actual_response = requests.get(
        url=BASE_URL + f"/collections/{collection_id}/position"
        f"?coords={coords}&parameter-name={parameters}&datetime={datetime}"
    )

    actual_status_code_is_expected_status_code(actual_response, 200)
    actual_response_is_expected_response(
        actual_response, "response/collection/position/200/single_coordinate_with_multiple_parameters.json"
    )


def test_from_a_single_collection_get_an_area_with_multiple_parameters():
    collection_id = "observations"
    coords = "POLYGON((5.0 52.0, 6.0 52.0,6.0 52.1,5.0 52.1, 5.0 52.0))"
    parameters = "dd,ff,rh"
    datetime = "2022-12-31T00:50:00Z/2022-12-31T02:10:00Z"
    actual_response = requests.get(
        url=BASE_URL + f"/collections/{collection_id}/area"
        f"?coords={coords}&parameter-name={parameters}&datetime={datetime}"
    )

    actual_status_code_is_expected_status_code(actual_response, 200)
    actual_response_is_expected_response(
        actual_response, "response/collection/area/200/data_within_an_area_with_multiple_parameters.json"
    )
