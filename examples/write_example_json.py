from src.file_metadata_parser.extract_metadata_netcdf import build_all_json_payloads_from_netCDF
from sys import argv
import xarray as xr
import json
import uuid
import subprocess
import os

"""
Small script to wirte json payloads instead of sending them to a mqtt-broker.
Supply path to netCDF file at commandline.

"""

if (project_root_path := subprocess.getoutput("git rev-parse --show-toplevel")) != os.getcwd():
    print(project_root_path)
    os.chdir(project_root_path)

path = argv[1]

ds = xr.load_dataset(path)

with open("schemas/netcdf_to_e_soh_message_metno.json", "r") as file:
	netcdf_def = json.load(file)

json_msg = build_all_json_payloads_from_netCDF(ds, netcdf_def)

json_msg = json_msg[-1]


#Only writes last message from the list of messages created in build_all_json_payloads_from_netCDF
json_msg["id"] = str(uuid.uuid4())
json_msg["version"] = "v04"

with open(f"examples/{path.split('/')[-1].strip('.nc')}_meta.json", "w") as file:
	file.write(json.dumps(json_msg, indent=4))