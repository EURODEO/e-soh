from src.file_metadata_parser.extract_metadata_netcdf import build_all_json_payloads_from_netCDF
from src.mapper import mapper

import pytest
import glob

import xarray as xr

from jsonschema import Draft202012Validator
import json


@pytest.mark.parametrize("netcdf_file_path", glob.glob("test/test_data/*.nc"))
def test_verify_json_payload_netcdf(netcdf_file_path):

    # Load the schema
    with open("schemas/e-soh-message-spec.json", "r") as file:
        e_soh_mqtt_message_schema = json.load(file)

    ds = xr.load_dataset(netcdf_file_path)

    select_map = mapper()

    json_payloads = build_all_json_payloads_from_netCDF(
        ds, select_map(ds.attrs["institution"]))

    for payload in json_payloads:
        assert Draft202012Validator(e_soh_mqtt_message_schema).validate(payload) is None


if __name__ == "__main__":
    test_verify_json_payload_netcdf(glob.glob("test/test_data/*nc"))
