from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr

import jinja2 as j2

import uuid
import json
import copy


def get_attrs(ds: xr.Dataset, var: str):
    """
    Get the attributes from the correct level in netCDF
    """
    metdata_dict = getattr(ds, var)
    if hasattr(metdata_dict, "attrs"):
        return metdata_dict.attrs
    elif hasattr(metdata_dict, "attr"):
        return metdata_dict.attr
    else:
        return metdata_dict


    
def create_json_from_netcdf_metdata(ds: xr.Dataset, map_netcdf: dict) -> str:
    """
    This function takes a netCDF file with ACDD and CF standard
    and creates a json string containing specified metadata fields
    in the e-soh-message-spec json schema. This function only extract the constant fields
    from the netcdf.

    Keyword arguemnts:
    path (xr.Dataset) -- An instance of a xr.Dataset loaded from a netCDF
    map_netcdf (dict) -- a json formated for datafield parsing from netCDF

    Return:
    str -- a json in string format

    """        

    def get_metadata_dict(json_message_target: dict, sub_map: dict) -> None:
        for netcdf_attr_target in sub_map["translation_fields"]:
            netcdf_metadata = get_attrs(ds, netcdf_attr_target)
            current_sub_dict = sub_map["translation_fields"][netcdf_attr_target]
            json_message_target.update(populate_json_message({}, netcdf_metadata, current_sub_dict))

            if "persistant_fields" in sub_map:
                json_message_target.update({i:netcdf_metadata[i] for i in sub_map["persistant_fields"][netcdf_attr_target]})

    def populate_json_message(json_message_target: dict, netcdf_metadata: dict, current_sub_dict: dict) -> dict:
        """
        """

        for key in current_sub_dict:
            if key == "inpt_type":
                continue
            match current_sub_dict[key]["inpt_type"]:
                case "str":
                    json_message_target[key] = f"{current_sub_dict[key]['sep']}".join([netcdf_metadata[i] for i in current_sub_dict[key]["fields"]])
                case "list":
                    json_message_target[key] = [netcdf_metadata[field] for field in current_sub_dict[key]["fields"]]
                case "raw":
                    json_message_target[key] = current_sub_dict[key]["value"]
                case "multi":
                    if key not in json_message_target:
                        json_message_target[key] = {}
                    
                    json_message_target[key].update(populate_json_message({},  netcdf_metadata, current_sub_dict[key]))

        return json_message_target



    def populate_links(json_message_target: dict, sub_map: dict) -> None:
        for netcdf_attr_target in sub_map["translation_fields"]:
            netcdf_metadata = get_attrs(ds, netcdf_attr_target)
            current_sub_dict = sub_map["translation_fields"][netcdf_attr_target]
            json_message_target += [populate_json_message({}, netcdf_metadata, i) for i in current_sub_dict]

            if "persistant_fields" in sub_map:
                pass
 


    message_json = {"properties": {}, "links": []}

    get_metadata_dict(message_json, map_netcdf["root"])
    get_metadata_dict(message_json["properties"], map_netcdf["properties"])
    populate_links(message_json["links"], map_netcdf["links"])


    return(json.dumps(message_json))



def build_all_json_payloads_from_netCDF(ds: xr.Dataset, mapping_json: dict) -> list[str]:
    json_msg = create_json_from_netcdf_metdata(ds, mapping_json)

    json_msg = json.loads(json_msg)

    json_msg["version"] = "v0.1"

    obs_var = ds.variables

    messages = []
    #select all datapoints from the last 24h of dataset timeseries
    ds_subset = ds.sel(time=slice(ds.time[-1] - np.timedelta64(1, "D"), ds.time[-1]))

    for obs_set in ds_subset:
        data = ds_subset[obs_set]
        for value, time in zip(data.data, data.time.data):

            time = np.datetime_as_string(time)
            json_msg["properties"]["datetime"] = time

            content_str = f"{value}"
            content = {
                "encoding": "utf-8",
                "standard_name": data.attrs["standard_name"],
                "unit": data.attrs["units"],
                "size": len(str.encode(content_str, "utf-8")),
                "value": content_str
            }

            json_msg["content"] = content
            
            #Set message publication time in RFC3339 format
            #Create UUID for the message, and state message format version
            json_msg["id"] = str(uuid.uuid4())
            current_time = datetime.utcnow().replace(microsecond=0)
            current_time_str = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')

            json_msg["properties"]["pubtime"] = f"{current_time_str[:-3]}{current_time_str[-3:].zfill(6)}Z"
    

            messages.append(copy.deepcopy(json_msg))


    # Returns all complete messages
    return messages



if __name__ == "__main__":
    print("Load METno data")
    ds = xr.load_dataset("../../test/test_data/air_temperature_gullingen_skisenter-parent.nc")
    with open("../../schemas/netcdf_to_e_soh_message_metno.json") as file:
        j_read_netcdf = json.load(file)

    print(create_json_from_netcdf_metdata(ds, j_read_netcdf), "\n\n\n\n")

    print("Load KNMI data")
    ds = xr.load_dataset("../../test/test_data/20221231.nc_bck")
    with open("../../schemas/netcdf_to_e_soh_message_knmi.json") as file:
        j_read_netcdf = json.load(file)

    print(create_json_from_netcdf_metdata(ds, j_read_netcdf))

