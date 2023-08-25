from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr

import uuid
import json
import copy


    
def create_json_from_netcdf_metdata(ds: xr.Dataset, map_netcdf: dict) -> str:
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
    
    """
    #Are we ever going to send polygon MQTT messages? or store polygons in datastore
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
    """





    """
    message_json = {
        "type": "Feature",
        "geometry":{"type": geometry_type,
            "coordinates": coords
            },
        "properties": {
            "data_id": ds.attrs["id"],
            "metadata_id": ds.attrs["naming_authority"]+":"+ds.attrs["id"],
        },
        "links": [
            {
                "href": ds.attrs["references"],
                "rel": "item",
                "type": "application/json"
            }
        ]
        }

    message_json["properties"].update({i:ds.attrs[i] for i in ["history","Conventions","keywords","title" ,"summary", "institution", "source", "creator_name", "creator_url", "creator_email", "institution", "license", "access_constraint"]})
    """

    message_json = {"properties": {}, "links": {}}

    message_json["geometry"] = {"type": "Point", "coordinates": [float(ds.attrs[i]) for i in map_netcdf["geometry"]]}

    #Get all fields from netCDF that need transformation before beeing added to the MQTT message
    for key in map_netcdf["root"]["translation_fields"]:
        match map_netcdf["root"]["translation_fields"][key][type]:
            case "str":
                message_json[key] = f"{map_netcdf['root']['translation_fields'][key]['sep']}".join([ds.attrs[i] for i in map_netcdf["root"]["translation_fields"][key]["fields"])
            case "list":
                message_json[key] = [ds.attrs[field] for field in map_netcdf["root"]["translation_fields"][key]["fields"]]
            



    for level in ["properties", "links"]:
        pass
    for key in map_netcdf["translation_fields"]:
        message_json["properties"][key] = map_netcdf["translation_fields"][key]["sep"].join([ds.attrs[i] for i in map_netcdf["translation_fields"][key]["fields"]])


    #Get all fields that do not need transformation
    message_json["properties"].update({i:ds.attrs[i] for i in map_netcdf["persistant_fields"]})

    message_json["links"] = {}

    for key in map_netcdf["links"]:
        message_json["links"].update({i:ds.attrs[map_netcdf["links"][key][i]] for i in map_netcdf["links"][key]})


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

