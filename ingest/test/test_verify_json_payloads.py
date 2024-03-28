import glob
import json

import pytest
import xarray as xr
from jsonschema import Draft202012Validator
from jsonschema import ValidationError

from api.ingest import IngestToPipeline
from ingest.bufr.bufresohmsg_py import init_bufr_schema_py
from ingest.bufr.bufresohmsg_py import init_bufrtables_py
from ingest.bufr.bufresohmsg_py import init_oscar_py


@pytest.mark.timeout(1000)
@pytest.mark.parametrize("bufr_file_path", glob.glob("test/test_data/bufr/*.buf*"))
def test_verify_json_payload_bufr(bufr_file_path):
    # Load the schema
    with open("src/ingest/schemas/e-soh-message-spec.json", "r") as file:
        e_soh_mqtt_message_schema = json.load(file)

    init_bufrtables_py("")
    init_oscar_py("./src/ingest/bufr/oscar/oscar_stations_all.json")
    init_bufr_schema_py("./src/ingest/schemas/bufr_to_e_soh_message.json")
    msg_build = IngestToPipeline(None, "testing", testing=True)
    with open(bufr_file_path, "rb") as file:
        bufr_content = file.read()

    json_payloads = msg_build._build_messages(bufr_content, input_type="bufr")

    for payload in json_payloads:
        try:
            assert Draft202012Validator(e_soh_mqtt_message_schema).validate(payload) is None
        except ValidationError as e:
            print(e)
            raise ValidationError(e)


@pytest.mark.parametrize("netcdf_file_path", glob.glob("test/test_data/met_norway/*.nc"))
def test_verify_json_payload_metno_netcdf(netcdf_file_path):
    # Load the schema
    with open("src/ingest/schemas/e-soh-message-spec.json", "r") as file:
        e_soh_mqtt_message_schema = json.load(file)

    ds = xr.load_dataset(netcdf_file_path)

    msg_build = IngestToPipeline(None, "testing", testing=True)

    json_payloads = msg_build._build_messages(ds, input_type="netCDF")

    for payload in json_payloads:
        try:
            assert Draft202012Validator(e_soh_mqtt_message_schema).validate(payload) is None
        except ValidationError as e:
            print(e)
            raise ValidationError(e)


# @pytest.mark.parametrize("netcdf_file_path", glob.glob("test/test_data/knmi/*.nc"))
# def test_verify_json_payload_knmi_netcdf(netcdf_file_path):
#     with open("src/ingest/schemas/e-soh-message-spec.json", "r") as file:
#         e_soh_mqtt_message_schema = json.load(file)

#     ds = xr.load_dataset(netcdf_file_path)

#     msg_build = IngestToPipeline(None, "testing", testing=True)

#     json_payloads = msg_build._build_messages(ds, input_type="netCDF")

#     for payload in json_payloads:
#         try:
#             assert Draft202012Validator(e_soh_mqtt_message_schema).validate(payload) is None
#         except ValidationError as e:
#             print(e, "\n\n")
#             raise ValidationError(e)


# if __name__ == "__main__":
#     [test_verify_json_payload_knmi_netcdf(i) for i in glob.glob("test/test_data/knmi/*nc")]
#     pass
