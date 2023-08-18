from extract_metadata_netcdf import create_json_from_netcdf_metdata
from datetime import datetime

import numpy as np
import xarray as xr

import uuid
import json


#Create all metadata for json-payload


def build_all_json_payloads_from_netCDF(path: str) -> list[str]:

    #Open the the netCDF
    ds = xr.open_dataset(path)

    json_msg = create_json_from_netcdf_metdata(ds)

    json_msg = json.loads(json_msg)

    json_msg["version"] = "v04"

    obs_var = ds.variables

    messages = []
    #select all datapoints from the last 24h of dataset timeseries
    ds_subset = ds.sel(time=slice(ds.time[-1] - np.timedelta64(1, "D"), ds.time[-1]))

    for obs_set in ds_subset:
        data = ds_subset[obs_set]
        for value, time in zip(data.data, data.time.data):
            content_str = f"{value}"

            content = {
                "encoding": "utf-8",
                "standard_name": data.attrs["standard_name"],
                "unit": data.attrs["units"],
                "size": len(str.encode(content_str, "utf-8")),
                "value": content_str
            }

            
            time = np.datetime_as_string(time)
            json_msg["start_datetime"] = time
            json_msg["end_datetime"] = time
    
            json_msg["content"] = content
            
            #Set message publication time in RFC3339 format
            #Create UUID for the message, and state message format version
            


            json_msg["id"] = str(uuid.uuid4())
            current_time = datetime.utcnow().replace(microsecond=0)
            current_time_str = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')

            json_msg["properties"]["pubtime"] = f"{current_time_str[:-3]}{current_time_str[-3:].zfill(6)}Z"
    

            messages.append(json_msg)



    return messages



if __name__ == "__main__":
    path = "../test_data/air_temperature_gullingen_skisenter-parent.nc"
    build_all_json_payloads_from_netCDF(path)


    


