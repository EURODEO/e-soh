import netCDF4 as nc
import datetime as dt
import numpy as np


class NetCDF:
    """Access to netCDF files"""

    def __init__(self, verbose):
        self._verbose = verbose

    def create_initial_file(self, path, ts):
        """Create the initial file for a time series."""

        with nc.Dataset(path, 'w') as dset:

            dset.setncatts({
                'station_id': ts.station_id(),
                'param_id': ts.param_id(),
                'spatial_representation': 'point',
                'geospatial_lat_min': ts.lat(),
                'geospatial_lat_max': ts.lat(),
                'geospatial_lon_min': ts.lon(),
                'geospatial_lon_max': ts.lon(),
            })

            vlat = dset.createVariable('latitude', 'f')
            vlat.standard_name = 'latitude'
            vlat.long_name = 'station latitude'
            vlat.units = 'degrees_north'
            vlat[:] = ts.lat()

            vlon = dset.createVariable('longitude', 'f')
            vlon.standard_name = 'longitude'
            vlon.long_name = 'station longitude'
            vlon.units = 'degrees_east'
            vlon[:] = ts.lon()

            dset.createDimension('time', 0)  # create time as an unlimited dimension

            v = dset.createVariable('time', 'i4', ('time',))
            # v[:] = times  --- no times yet (set in replace_times_and_obs())
            v.standard_name = 'time'
            v.long_name = 'Time of measurement'
            v.calendar = 'standard'
            ref_dt = dt.datetime.strptime('1970-01-01', '%Y-%m-%d').replace(tzinfo=dt.timezone.utc)
            v.units = f"seconds since {ref_dt.strftime('%Y-%m-%d %H:%M:%S')}"
            v.axis = 'T'

            v = dset.createVariable(ts.param_id(), 'f4', ['time'])
            # v[:] = obs_values  --- no obs_values yet (set in replace_times_and_obs())
            v.standard_name = ts.param_id()  # for now
            v.long_name = '{} (long name)'.format(ts.param_id())  # for now
            v.coordinates = 'time latitude longitude'
            v.coverage_content_type = 'physicalMeasurement'

    def replace_times_and_obs(self, path, times, obs):
        """Replace contents of times and obs variables in file."""

        with nc.Dataset(path, 'a') as dset:
            dset['time'][:] = times
            dset[dset.param_id][:] = obs

    def get_times_and_obs(self, path, from_time, to_time):
        """Retrieve contents of times and obs variables from file within [from_time, to_time>.

        Returns two lists: times and obs (subject to the same restrictions as
        StorageBackend.set_obs())
        """

        with nc.Dataset(path, 'r') as dset:
            time_var = dset.variables['time']
            indices = np.where((time_var[:] >= from_time) & (time_var[:] < to_time))
            return list(time_var[indices]), list(dset.variables[dset.param_id][indices])
