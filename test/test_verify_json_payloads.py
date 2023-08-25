import os
import subprocess
import sys


import pytest
import glob

import xarray as xr

from jsonschema import Draft202012Validator
import json

#Set current working path and add top level of repo to path
#if (project_root_path := subprocess.getoutput("git rev-parse --show-toplevel")) != os.getcwd():
#    print(project_root_path)
#    os.chdir(project_root_path)
#
print("Current work dir")
print(os.getcwd())


from src.file_metadata_parser.extract_metadata_netcdf import build_all_json_payloads_from_netCDF
@pytest.mark.parametrize("netcdf_file_path", glob.glob("test/test_data/*.nc"))
def test_verify_json_payload_netcdf(netcdf_file_path):
    
    #Load the schema
    with open("schemas/e-soh-message-spec.json", "r") as file:
        e_soh_mqtt_message_schema = json.load(file)
    
    json_payloads = build_all_json_payloads_from_netCDF(xr.load_dataset(netcdf_file_path))

    for payload in json_payloads:
        assert Draft202012Validator(e_soh_mqtt_message_schema).is_valid(payload)


if __name__ == "__main__":
    test_verify_json_payload_netcdf(glob.glob("test/test_data/*nc"))