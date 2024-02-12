import json

import datastore_pb2 as dstore
from google.protobuf.json_format import Parse


def create_mock_obs_response(json_data):
    response = dstore.GetObsResponse()
    Parse(json.dumps(json_data), response)
    return response


def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)
