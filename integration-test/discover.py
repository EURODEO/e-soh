# This code was used to double-check the values tested in test_knmi.py
from pathlib import Path

import xarray as xr

file_path = Path(Path(__file__).parents[1] / "test-data" / "KNMI" / "20221231.nc")
with xr.open_dataset(
    file_path, engine="netcdf4", chunks=None
) as ds:  # chunks=None to disable dask
    # print(ds)
    print(ds.sel(station="06260").isel(time=0).lat.values)
    print(ds.sel(station="06260").isel(time=0).lon.values)

    print(ds.dims)

    print(ds.sel(station="06260").rh.values)
