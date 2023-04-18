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

    def set_obs(self, ts, times, obs):
        """See documentation in base class."""

        path = '{}/{}/{}/{}'.format(self._nc_dir, ts.station_id(), ts.param_id(), self._nc_fname)
        self._netcdf.replace_times_and_obs(path, times, obs)

    def add_obs(self, ts, times, obs):
        """See documentation in base class."""
        # TODO

    def get_obs(self, tss, from_time, to_time):
        """See documentation in base class."""
        # TODO

    def get_tss_in_circle(self, lat, lon, radius):
        """See documentation in base class."""
        # TODO
