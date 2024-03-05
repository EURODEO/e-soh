import json

import datastore_pb2 as dstore
from deepdiff import DeepDiff
from formatters.geojson import convert_to_geojson
from google.protobuf import json_format


def actual_response_is_expected_response(actual_response, expected_path, **kwargs):
    # file_path = Path(Path(__file__).parent, expected_path).resolve()
    with open(expected_path) as file:
        expected_json = json.load(file)

    diff = DeepDiff(expected_json, actual_response.json(), **kwargs)
    assert diff == {}


def test_geojson_conversion():
    ts_response = dstore.GetObsResponse()
    with open("test/test_data/test_ts_geojson_proto_object.json") as file:
        json_format.ParseDict(json.load(file), ts_response)

    result = convert_to_geojson(ts_response)
    print(result)
    # collections/observations/items?bbox=5.7,52.0,6.0,52.059&
    # parameter-name=rainfall_amount_2.0_point_PT24H,
    #                air_temperature_2.0_maximum_PT10M,
    #                total_downwelling_shortwave_flux_in_air_2.0_mean_PT10M
    expected = "test/test_data/test_ts_expected_geojson.json"
    actual_response_is_expected_response(result, expected)
