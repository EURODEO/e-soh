# Test covjson.py
import json

import datastore_pb2 as dstore
import pytest
from covjson_pydantic.coverage import Coverage
from covjson_pydantic.coverage import CoverageCollection
from fastapi import HTTPException
from formatters.covjson import Covjson
from google.protobuf.json_format import Parse


def test_single_parameter_convert():
    test_data = load_json("test/test_data/test_single.json")
    compare_data = load_json("test/test_data/test_single_compare.json")

    response = create_mock_obs_response(test_data)
    coverage_collection = Covjson().convert(response)

    assert coverage_collection is not None

    assert type(coverage_collection) is Coverage

    # Assert that the coverage collection has the correct parameter
    # TODO: Change parameter name when parameter names have been decided
    assert "ff" in coverage_collection.parameters.keys()

    # Check that correct values exist in the coverage collection
    assert 9.21 in coverage_collection.ranges["ff"].values

    # compare the coverage collection with the compare data
    # TODO: Modify compare data when parameter names have been decided
    coverage_collection_json = json.loads(coverage_collection.model_dump_json(exclude_none=True))
    assert coverage_collection_json == compare_data


def test_multiple_parameter_convert():
    test_data = load_json("test/test_data/test_multiple.json")
    compare_data = load_json("test/test_data/test_multiple_compare.json")

    response = create_mock_obs_response(test_data)

    coverage_collection = Covjson().convert(response)

    assert coverage_collection is not None

    assert type(coverage_collection) is Coverage

    # Check that the coverage collection has the correct parameters
    # TODO: Change parameter names when parameter names have been decided
    assert all([parameter in ["dd", "ff", "rh", "pp", "tn"] for parameter in coverage_collection.parameters.keys()])

    # Check that correct values exist in the coverage collection
    assert 230.7 in coverage_collection.ranges["dd"].values
    assert 1000.48 in coverage_collection.ranges["pp"].values

    # TODO: Modify compare data when parameter names have been decided
    coverage_collection_json = json.loads(coverage_collection.model_dump_json(exclude_none=True))
    assert coverage_collection_json == compare_data


def test_single_parameter_area_convert():
    test_data = load_json("test/test_data/test_coverages.json")
    compare_data = load_json("test/test_data/test_coverages_compare.json")

    response = create_mock_obs_response(test_data)

    coverage_collection = Covjson().convert(response)

    assert coverage_collection is not None

    assert type(coverage_collection) is CoverageCollection

    assert len(coverage_collection.coverages) > 1

    assert all([type(coverage) is Coverage for coverage in coverage_collection.coverages])

    # Check that each coverage has the correct parameter
    # TODO: Change parameter name when parameter names have been decided
    assert all(["TA_P1D_AVG" in coverage.parameters.keys() for coverage in coverage_collection.coverages])

    # TODO: Modify compare data when parameter names have been decided
    coverage_collection_json = json.loads(coverage_collection.model_dump_json(exclude_none=True))
    assert coverage_collection_json == compare_data


def test_empty_response_convert():
    test_data = load_json("test/test_data/test_empty.json")
    response = create_mock_obs_response(test_data)

    # Expect to get an HTTPException when converting an empty response
    with pytest.raises(HTTPException):
        Covjson().convert(response)


def create_mock_obs_response(json_data):
    response = dstore.GetObsResponse()
    Parse(json.dumps(json_data), response)
    return response


def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)