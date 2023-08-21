from extract_metadata_netcdf import create_json_from_netcdf_metdata
from create_mqtt_message import build_all_json_payloads_from_netCDF
from sys import argv
import xarray as xr
import json
import uuid

"""
Small script to wirte json payloads instead of sending them to a mqtt-broker.
Supply path to netCDF file at commandline.

"""

path = argv[1]

ds = xr.load_dataset(path)

json_msg = build_all_json_payloads_from_netCDF(ds)


#Only writes last message from the list of messages created in build_all_json_payloads_from_netCDF
json_msg = json.loads(json_msg[-1])

json_msg["id"] = str(uuid.uuid4())
json_msg["version"] = "v04"

with open(f"{path.split('/')[-1].strip('.nc')}_meta.json", "w") as file:
	file.write(json.dumps(json_msg, indent=4))