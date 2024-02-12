import json

import pytest
from covjson_pydantic.coverage import Coverage
from covjson_pydantic.coverage import CoverageCollection
from fastapi import HTTPException
from formatters.covjson import convert_to_covjson
from test.utilities import create_mock_obs_response
from test.utilities import load_json


def test_single_parameter_convert():
    test_data = load_json("test/test_data/test_single_proto.json")
    compare_data = load_json("test/test_data/test_single_covjson.json")

    response = create_mock_obs_response(test_data)
    coverage_collection = convert_to_covjson(response)

    assert coverage_collection is not None

    assert type(coverage_collection) is Coverage

    # Assert that the coverage collection has the correct parameter
    # TODO: Change parameter name when parameter names have been decided
    assert "ff" in coverage_collection.parameters.keys()

    # Check that correct values exist in the coverage collection
    assert 9.21 in coverage_collection.ranges["ff"].values

    assert len(coverage_collection.domain.axes.t.values) == 7

    # Number of time points should match with the number of observation values
    assert len(coverage_collection.domain.axes.t.values) == len(coverage_collection.ranges["ff"].values)

    # compare the coverage collection with the compare data
    # TODO: Modify compare data when parameter names have been decided
    coverage_collection_json = json.loads(coverage_collection.model_dump_json(exclude_none=True))
    assert coverage_collection_json == compare_data


def test_multiple_parameter_convert():
    test_data = load_json("test/test_data/test_multiple_proto.json")
    compare_data = load_json("test/test_data/test_multiple_covjson.json")

    response = create_mock_obs_response(test_data)

    coverage_collection = convert_to_covjson(response)

    assert coverage_collection is not None

    assert type(coverage_collection) is Coverage

    # Check that the coverage collection has the correct parameters
    # TODO: Change parameter names when parameter names have been decided
    assert set(["dd", "ff", "rh"]) == coverage_collection.parameters.keys()

    # Check that correct values exist in the coverage collection
    assert 230.7 in coverage_collection.ranges["dd"].values
    assert 9.19 in coverage_collection.ranges["ff"].values
    assert 88.0 in coverage_collection.ranges["rh"].values

    # TODO: Modify compare data when parameter names have been decided
    coverage_collection_json = json.loads(coverage_collection.model_dump_json(exclude_none=True))
    assert coverage_collection_json == compare_data


def test_single_parameter_area_convert():
    test_data = load_json("test/test_data/test_coverages_proto.json")
    compare_data = load_json("test/test_data/test_coverages_covjson.json")

    response = create_mock_obs_response(test_data)

    coverage_collection = convert_to_covjson(response)

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
    test_data = load_json("test/test_data/test_empty_proto.json")
    response = create_mock_obs_response(test_data)

    # Expect to get an HTTPException with status code of 404 and detail of
    # "No data found" when converting an empty response
    with pytest.raises(HTTPException) as exception_info:
        convert_to_covjson(response)

    assert exception_info.value.detail == "No data found"
    assert exception_info.value.status_code == 404
