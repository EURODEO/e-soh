from extract_metadata_netcdf import create_json_from_netcdf_metdata
import json
import uuid

path = "../test_data/SN99938.nc"

json_msg = create_json_from_netcdf_metdata(path)

json_msg = json.loads(json_msg)

json_msg["id"] = str(uuid.uuid4())
json_msg["version"] = "v04"

with open(f"{path.split('/')[-1].strip('.nc')}_meta.json", "w") as file:
	file.write(json.dumps(json_msg, indent=4))