from storagebackend import StorageBackend
from postgissbe import PostGISSBE
import shutil
from pathlib import Path
from netcdf import NetCDF
import sys


class NetCDFSBE_TSMDataInPostGIS(StorageBackend):
    """A storage backend that uses netCDF files on the local file system for storage
    of observations and per observation metadata, and a PostGIS database for keeping per time
    series metadata.

    There will be one netCDF file per time series (station/param combo). Each such file will
    contain observations as well as all metadata; both per observation and per time series.
    The rationale for keeping per time series metadata in PostGIS is to provide a faster search
    for relevant time series. The actual observations are then be retrieved from the netCDF
    files in a second step.

    Files will be organized like this under self._nc_dir:

        station_id1/
           param_id1/
              data.nc
           param_id2/
              data.nc
           ...
        station_id2/
           ...
    """

    def __init__(self, verbose, pg_conn_info, nc_dir):
        super().__init__(verbose, 'netCDF (time series metadata in PostGIS)')
        self._pgsbe = PostGISSBE(verbose, pg_conn_info)  # for keeping per time series metadata
        self._nc_dir = nc_dir  # directory under which to keep the netCDF files
        self._netcdf = NetCDF(verbose)
        self._nc_fname = 'data.nc'

    def reset(self, tss):
        """See documentation in base class."""

        if self._verbose:
            print(
                'resetting NetCDFSBE_TSMDataInPostGIS with {} time series'.format(len(tss)),
                file=sys.stderr)

        self._pgsbe.reset(tss)

        # wipe any existing directory
        shutil.rmtree(self._nc_dir, ignore_errors=True)

        # create files with all per time series metadata, but with no observations
        for ts in tss:
            # create directory
            target_dir = '{}/{}/{}'.format(self._nc_dir, ts.station_id(), ts.param_id())
            Path(target_dir).mkdir(parents=True, exist_ok=True)

            # create initial file
            self._netcdf.create_initial_file('{}/{}'.format(target_dir, self._nc_fname), ts)

    def set_obs(self, ts, times, values):
        """See documentation in base class."""

        path = '{}/{}/{}/{}'.format(self._nc_dir, ts.station_id(), ts.param_id(), self._nc_fname)
        self._netcdf.replace_times_and_values(path, times, values)

    def add_obs(self, ts, times, values, oldest_time=None):
        """See documentation in base class."""

        path = '{}/{}/{}/{}'.format(self._nc_dir, ts.station_id(), ts.param_id(), self._nc_fname)
        self._netcdf.add_times_and_values(path, times, values, oldest_time)

    def get_obs(self, ts_ids, from_time, to_time):
        """See documentation in base class."""
        # TODO

    def get_obs_all(self, from_time, to_time):
        """See documentation in base class."""

        ts_ids = self.get_ts_ids_all()

        res = []
        for ts_id in ts_ids:
            station_id, param_id = self._pgsbe.get_station_and_param(ts_id)
            path = '{}/{}/{}/{}'.format(self._nc_dir, station_id, param_id, self._nc_fname)
            times, values = self._netcdf.get_times_and_values(path, from_time, to_time)
            res.append((ts_id, times[:], values[:]))

        return res

    def get_station_and_param(self, ts_id):
        """See documentation in base class."""

        return self._pgsbe.get_station_and_param(ts_id)

    def get_ts_ids_all(self):
        """See documentation in base class."""

        return self._pgsbe.get_ts_ids_all()

    def get_ts_ids_in_circle(self, lat, lon, radius):
        """See documentation in base class."""

        return self._pgsbe.get_ts_ids_in_circle(lat, lon, radius)
