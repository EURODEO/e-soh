import xarray as xr
import json

    
def create_json_from_netcdf_metdata(ds: xr.Dataset) -> str:
    """
    This function takes a netCDF file with ACDD and CF standard
    and creates a json string containing specified metadata fields
    in the e-soh-message-spec json schema.

    Keyword arguemnts:
    path (str) -- path to the netcdf file

    Return:
    str -- a json in string format

    Raises:
    Raises error if the geometry type from netCDF attribute geospatial_bounds
    are unknown.
    """        
    
    #ds = xr.open_dataset(path)

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
            "keywords": ds.attrs["keywords"].split(","),
            "Conventions": ds.attrs["Conventions"].split(","),
            "history": ds.attrs["history"].split("\n")
            
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






