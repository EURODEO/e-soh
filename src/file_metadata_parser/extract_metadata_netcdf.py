from datetime import datetime

import numpy as np
import xarray as xr

import uuid
import json
import copy
    
def create_json_from_netcdf_metdata(ds: xr.Dataset) -> str:
    """
    This function takes a netCDF file with ACDD and CF standard
    and creates a json string containing specified metadata fields
    in the e-soh-message-spec json schema.

    Keyword arguemnts:
    path (xr.Dataset) -- An instance of a xr.Dataset loaded from a netCDF

    Return:
    str -- a json in string format

    Raises:
    Raises error if the spatial_representation format is unrecognized.
    """        
    
    if (geometry_type := ds.attrs["spatial_representation"]) == "point":
        geometry_type = "Point"
        coords = [float(ds.attrs["geospatial_lat_min"]), float(ds.attrs["geospatial_lon_min"])]
    elif geometry_type == "polygon":
        geometry_type = "Polygon"
        coords = [[float(ds.attrs["geospatial_lat_min"]), float(ds.attrs["geospatial_lon_min"])],
                  [float(ds.attrs["geospatial_lat_min"]), float(ds.attrs["geospatial_lon_max"])],
                  [float(ds.attrs["geospatial_lat_max"]), float(ds.attrs["geospatial_lon_min"])],
                  [float(ds.attrs["geospatial_lat_max"]), float(ds.attrs["geospatial_lon_max"])]]

        
    else:
        raise ValueError("Unknown geometry type")

    message_json = {
        "type": "Feature",
        "geometry":{"type": geometry_type,
            "coordinates": coords
            },
        "properties": {
            "title": ds.attrs["title"],
            "data_id": ds.attrs["id"],
            "metadata_id": ds.attrs["naming_authority"]+":"+ds.attrs["id"],
            "keywords": ds.attrs["keywords"],
            "Conventions": ds.attrs["Conventions"],
            "history": ds.attrs["history"]
            
        },
        "links": [
            {
                "href": ds.attrs["references"],
                "rel": "item",
                "type": "application/json"
            }
        ]
        }

    message_json["properties"].update({i:ds.attrs[i] for i in ["summary", "institution", "source", "creator_name", "creator_url", "creator_email", "institution", "license", "access_constraint"]})



    return(json.dumps(message_json))




def build_all_json_payloads_from_netCDF(ds: xr.Dataset) -> list[str]:
    json_msg = create_json_from_netcdf_metdata(ds)

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

