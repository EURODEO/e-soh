from storagebackend import StorageBackend


class NetCDF(StorageBackend):
    """A storage backend that uses netCDF files on the local file system for storage.

    There will be one netCDF file per time series (station/param combo).
    """

    def __init__(self, verbose):
        super().__init__(verbose)
        # TODO: pass directory in which to keep files to __init__

    def reset(self, tss):
        """See documentation in base class."""
        if self._verbose:
            print('\nresetting NetCDF SBE with {} time series ... TODO'.format(len(tss)))
        # TODO:
        # - delete all files
        # - create files with all ts-specific metadata, but with no observations

    def set_observations(self, ts, obs):
        """See documentation in base class."""
        # TODO:
        # - replace contents of time and observation variables in file
        pass
