import xarray as xr
import json
import hashlib
import subprocess

def hash_file(file_path: str, hash_method: str) -> str:
    """
    Calls the openssl hash program found on om most linux instralation.

    Keyword arguemnts:
    file_path (str) -- path to file that need hashing
    hash_method (str) -- hashing algorithm used to has file

    Return:
    str -- Returns a string with the file hash
    """
	
    return str(subprocess.check_output(["openssl", hash_method, file_path])).split("=")[-1].strip().strip("\\n'")
    
def create_json_from_netcdf_metdata(path: str, integrity_hash: str = "sha256") -> str:
    """
    This function takes a netCDF file with ACDD and CF standard
    and creates a json string containing specified metadata fields
    in the e-soh-message-spec json schema.

    Keyword arguemnts:
    path (str) -- path to the netcdf file
    integrity_hash (str) -- hashing method used to create the file fingerprint,
                      supported methods are sha256, sha384, sha512, sha3-256,
                      sha3-384 and sha3-512

    Return:
    str -- a json in string format

    Raises:
    Raises error if the geometry type from netCDF attribute geospatial_bounds
    are unknown.
    """        


    path = "../test_data/SN99938.nc"
    ds = xr.open_dataset(path)

    geospatial = ds.attrs["geospatial_bounds"]

    geospatial = geospatial.replace("(", " ").replace(")", "").split(" ")


    geometry_type = geospatial[0]

    if geometry_type == "POINT":
        geometry_type = "Point"
        coords = [float(i) for i in geospatial[1:]]
    elif geometry_type == "POLYGON":
        raise NotImplementedError("Handling of polygons not yet implemented.")
        
    else:
        raise ValueError("Unknown geometry type")

    ds.attrs["geospatial_bounds"]

    message_json = {
        "type": "Feature",
        "geometry":{"type": geometry_type,
            "coordinates": coords
            },
        "properties": {
            "pubtime": ds.attrs["date_created"],
            "title": ds.attrs["title"],
            "data_id": ds.attrs["id"],
            "start_datetime": str(ds["time"].min().data),
            "end_datetime": str(ds["time"].max().data),
            "integrity": {"method": "sha256", 
                        "value": hash_file(path, "sha256")},
            "keywords": ds.attrs["keywords"].split(","),
            "Conventions": ds.attrs["Conventions"].split(","),
            "history": ds.attrs["history"].split("\n")
            
        },
        "links": [
            {
                "href": "placeholder, this need to be set up when we have decided about datasources, should link to where the data set can be downloaded, and for multiple applications such as json, netCDF or buffr",
                "rel": "item",
                "type": "application/json"
            }
        ]
        }

    message_json["properties"].update({i:ds.attrs[i] for i in ["summary", "institution", "source", "creator_name", "creator_url", "creator_email", "institution"]})



    return(json.dumps(message_json))






