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


def actual_response_is_expected_response(actual_response, expected_path, **kwargs):
    file_path = Path(Path(__file__).parent, expected_path).resolve()
    with open(file_path) as file:
        expected_json = json.load(file)

    diff = DeepDiff(expected_json, actual_response.json(), **kwargs)
    assert diff == {}
    # TODO: maybe this check should be a unit test instead of integration.
    # Deep diff does not check dict keys for order, manually validate if the order is correct.
    validate_if_the_dict_keys_are_in_alphabetic_order(actual_response=actual_response, expected_path=expected_path)

def validate_if_the_dict_keys_are_in_alphabetic_order(actual_response, expected_path):
    """Python dictionaries used to be unordered. Resulting that the keys of dictionaries are not checked for the right
    order. Therefore dictionaries where the parameter names are the keys are checked manually for the correct sequence
    in this function."""
    actual_json = actual_response.json()
    file_path = Path(Path(__file__).parent, expected_path).resolve()
    with open(file_path) as file:
        expected_json = json.load(file)

        if actual_json.get("ranges") or expected_json.get("ranges"):
            actual_ranges = actual_json["ranges"]
            expected_ranges = expected_json["ranges"]
            assert list(actual_ranges.keys()) == list(expected_ranges.keys())
        if actual_json.get("parameters") or expected_json.get("parameters"):
            actual_parameters = actual_json["parameters"]
            expected_parameters = expected_json["parameters"]
            assert list(actual_parameters.keys()) == list(expected_parameters.keys())
        if actual_json.get("coverages") or expected_json.get("coverages"):
            actual_coverages = actual_json["coverages"]
            expected_coverages = expected_json["coverages"]
            for actual_covjson, expected_covjson in zip(actual_coverages, expected_coverages):
                assert list(actual_covjson["parameters"].keys()) == list(expected_covjson["parameters"].keys())

def test_get_all_collections():
    actual_response = requests.get(url=BASE_URL + "/collections")

    assert actual_response.status_code == 200
    actual_response_is_expected_response(
        actual_response, "response/capabilities/200/all_collections.json", exclude_regex_paths=r"\['href'\]$"
    )


def test_get_a_single_existing_collection():
    collection_id = "observations"
    actual_response = requests.get(url=BASE_URL + f"/collections/{collection_id}")

    assert actual_response.status_code == 200
    actual_response_is_expected_response(
        actual_response, "response/metadata/200/single_collection.json", exclude_regex_paths=r"\['href'\]$"
    )


def test_get_a_collection_which_does_not_exist():
    collection_id = "does-not-exist"
    actual_response = requests.get(url=BASE_URL + f"/collections/{collection_id}")

    assert actual_response.status_code == 404
    actual_response_is_expected_response(actual_response, "response/metadata/404/not_found.json")


def test_from_a_single_collection_get_locations_within_a_bbox():
    collection_id = "observations"
    bbox = "5.0,52.0,6.0,52.1"
    actual_response = requests.get(url=BASE_URL + f"/collections/{collection_id}/locations?bbox={bbox}")

    assert actual_response.status_code == 200
    actual_response_is_expected_response(
        actual_response, "response/collection/locations/200/locations_within_a_bbox.json"
    )


def test_from_a_single_collection_get_a_single_location():
    collection_id = "observations"
    location_id = "06260"
    parameters = (
        "air_temperature_1.5_maximum_PT10M , wind_from_direction_2.0_mean_PT10M,relative_humidity_2.0_mean_PT1M"
    )
    datetime = "../2022-12-31T01:10:00Z"
    actual_response = requests.get(
        url=BASE_URL + f"/collections/{collection_id}/locations/{location_id}"
        f"?parameter-name={parameters}&datetime={datetime}"
    )

    assert actual_response.status_code == 200
    actual_response_is_expected_response(
        actual_response, "response/collection/locations/200/single_location_with_multiple_parameters.json"
    )


def test_that_the_order_of_the_parameters_in_the_response_is_always_the_same():
    """Test that we do not care about the order of parameters passed in the query.
    By comparing two requests with the same parameters but in a different sequence.
    The first request returns the same response as the second request.
    """
    collection_id = "observations"
    location_id = "06260"
    parameters = " wind_from_direction_2.0_mean_PT10M,wind_speed_10_mean_PT10M ,  relative_humidity_2.0_mean_PT1M"
    first_response = requests.get(
        url=BASE_URL + f"/collections/{collection_id}/locations/{location_id}" f"?parameter-name={parameters}"
    )

    parameters_2 = (
        " relative_humidity_2.0_mean_PT1M, wind_speed_10_mean_PT10M,   wind_from_direction_2.0_mean_PT10M    "
    )
    second_response = requests.get(
        url=BASE_URL + f"/collections/{collection_id}/locations/{location_id}" f"?parameter-name={parameters_2}"
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    diff = DeepDiff(first_response.json(), second_response.json())
    assert diff == {}


def test_from_a_single_collection_get_a_single_location_which_does_not_exist():
    collection_id = "observations"
    location_id = "does-not-exist"
    parameters = "does-not-exist"
    actual_response = requests.get(
        url=BASE_URL + f"/collections/{collection_id}/locations/{location_id}?parameter-name={parameters}"
    )

    assert actual_response.status_code == 404
    actual_response_is_expected_response(actual_response, "response/collection/locations/404/no_data_found.json")


def test_from_a_single_collection_get_a_single_position_with_one_parameter():
    collection_id = "observations"
    coords = "POINT(5.179705 52.0988218)"
    parameters = "air_temperature_1.5_maximum_PT10M"
    datetime = "2022-12-31T00:50:00Z/2022-12-31T02:10:00Z"
    actual_response = requests.get(
        url=BASE_URL + f"/collections/{collection_id}/position"
        f"?coords={coords}&parameter-name={parameters}&datetime={datetime}"
    )

    assert actual_response.status_code == 200
    actual_response_is_expected_response(
        actual_response, "response/collection/position/200/single_coordinate_with_one_parameter.json"
    )


def test_from_a_single_collection_get_an_area_with_two_parameters():
    collection_id = "observations"
    coords = "POLYGON((5.0 52.0, 6.0 52.0,6.0 52.1,5.0 52.1, 5.0 52.0))"
    parameters = "relative_humidity_2.0_mean_PT1M ,   wind_speed_10_mean_PT10M"
    datetime = "2022-12-31T22:50:00Z/.."
    actual_response = requests.get(
        url=BASE_URL + f"/collections/{collection_id}/area"
        f"?coords={coords}&parameter-name={parameters}&datetime={datetime}"
    )

    assert actual_response.status_code == 200
    actual_response_is_expected_response(
        actual_response, "response/collection/area/200/data_within_an_area_with_two_parameters.json"
    )
