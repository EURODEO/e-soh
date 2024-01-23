# Test covjson.py
import json

import datastore_pb2 as dstore
from covjson_pydantic.coverage import Coverage
from covjson_pydantic.coverage import CoverageCollection
from formatters.covjson import Covjson
from google.protobuf.json_format import Parse


def test_convert():
    test_data = load_json("test/test_data/test1.json")

    response = create_mock_obs_response(test_data)
    coverage_collection = Covjson().convert(response)

    assert coverage_collection is not None

    assert type(coverage_collection) is CoverageCollection

    assert len(coverage_collection.coverages) > 0

    assert all(map(lambda cov: isinstance(cov, Coverage), coverage_collection.coverages))

    # TODO: Add more assertion


# TODO: Add more tests


def create_mock_obs_response(json_data):
    response = dstore.GetObsResponse()
    Parse(json.dumps(json_data), response)
    return response


def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)
