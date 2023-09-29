from esoh.ingest.main import ingest_to_pipeline

import pytest
import glob

import xarray as xr

from jsonschema import Draft202012Validator
import json


@pytest.mark.parametrize("netcdf_file_path", glob.glob("test/test_data/met_norway/*.nc"))
def test_verify_json_payload_metno_netcdf(netcdf_file_path):
    # Load the schema
    with open("schemas/e-soh-message-spec.json", "r") as file:
        e_soh_mqtt_message_schema = json.load(file)

    ds = xr.load_dataset(netcdf_file_path)

    msg_build = ingest_to_pipeline(None, "testing", testing=True)

    json_payloads = msg_build.build_messages(ds, input_type="netCDF")

    for payload in json_payloads:
        assert Draft202012Validator(e_soh_mqtt_message_schema).validate(payload) is None


@pytest.mark.parametrize("netcdf_file_path", glob.glob("test/test_data/knmi/*.nc"))
def test_verify_json_payload_knmi_netcdf(netcdf_file_path):
    with open("schemas/e-soh-message-spec.json", "r") as file:
        e_soh_mqtt_message_schema = json.load(file)

    ds = xr.load_dataset(netcdf_file_path)

    msg_build = ingest_to_pipeline(None, "testing", testing=True)

    json_payloads = msg_build.build_messages(ds, input_type="netCDF")

    for payload in json_payloads:
        assert Draft202012Validator(e_soh_mqtt_message_schema).validate(payload) is None


if __name__ == "__main__":
    [test_verify_json_payload_knmi_netcdf(i) for i in glob.glob("test/test_data/knmi/*nc")]
    pass
