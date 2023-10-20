import datetime as dt

import common
import netCDF4 as nc  # noqa: N813
import numpy as np


class NetCDF:
    """Access to netCDF files"""

    def __init__(self, verbose):
        self._verbose = verbose

    def create_initial_file(self, path, ts):
        """Create the initial file for a time series."""

        with nc.Dataset(path, "w") as dset:
            dset.setncatts(
                {
                    "station_id": ts.station_id(),
                    "param_id": ts.param_id(),
                    "spatial_representation": "point",
                    "geospatial_lat_min": ts.lat(),
                    "geospatial_lat_max": ts.lat(),
                    "geospatial_lon_min": ts.lon(),
                    "geospatial_lon_max": ts.lon(),
                }
            )

            vlat = dset.createVariable("latitude", "f")
            vlat.standard_name = "latitude"
            vlat.long_name = "station latitude"
            vlat.units = "degrees_north"
            vlat[:] = ts.lat()

            vlon = dset.createVariable("longitude", "f")
            vlon.standard_name = "longitude"
            vlon.long_name = "station longitude"
            vlon.units = "degrees_east"
            vlon[:] = ts.lon()

            dset.createDimension("time", 0)  # create time as an unlimited dimension

            v = dset.createVariable("time", "i4", ("time",))
            v.standard_name = "time"
            v.long_name = "Time of measurement"
            v.calendar = "standard"
            ref_dt = dt.datetime.strptime("1970-01-01", "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
            v.units = f"seconds since {ref_dt.strftime('%Y-%m-%d %H:%M:%S')}"
            v.axis = "T"

            v = dset.createVariable("value", "f4", ["time"])
            v.standard_name = ts.param_id()  # for now
            v.long_name = "{} (long name)".format(ts.param_id())  # for now
            v.coordinates = "time latitude longitude"
            v.coverage_content_type = "physicalMeasurement"

    def replace_times_and_values(self, path, times, values):
        """Replace contents of 'time' and 'value' variables in file."""

        with nc.Dataset(path, "a") as dset:
            dset["time"][:] = times
            dset["value"][:] = values

    def add_times_and_values(self, path, times, values, oldest_time=None):
        """Add new or replace/remove observations in file.


        If oldest_time is not None, existing observations older than this will be removed.

        """

        # OBSOLETE
        # # convert addable lists to np.array
        # times_add = np.array(times)
        # values_add = np.array(values)

        with nc.Dataset(path, "a") as dset:
            # retrieve file variables
            ftimes = dset["time"][:]
            fvalues = dset["value"][:]

            # merge
            mtimes, mvalues = common.ts_merge(
                ftimes.tolist(), fvalues.tolist(), times, values, oldest_time
            )

            # replace file variables with merged arrays
            dset["time"][:] = mtimes
            dset["value"][:] = mvalues

    def get_times_and_values(self, path, from_time, to_time):
        """Retrieve contents of 'time' and 'value' variables from file within [from_time, to_time>.

        Returns two lists: times and values (subject to the same restrictions as
        StorageBackend.set_obs())
        """

        with nc.Dataset(path, "r") as dset:
            time_var = dset.variables["time"]
            indices = np.where((time_var[:] >= from_time) & (time_var[:] < to_time))
            return list(time_var[indices]), list(dset.variables["value"][indices])
