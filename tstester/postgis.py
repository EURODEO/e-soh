from storagebackend import StorageBackend


class PostGIS(StorageBackend):
    """A storage backend that uses a PostGIS instance for storage."""

    def __init__(self, verbose):
        super().__init__(verbose)
        # TODO: pass database connection info (host/port/user/passwd/database etc.) to __init__

    def reset(self, tss):
        """See documentation in base class."""
        if self._verbose:
            print('\nresetting PostGIS SBE with {} time series'.format(len(tss)))
        # TODO:
        # - drop database
        # - create schema based on info in tss
        # - insert rows in time series table

    def set_observations(self, ts, obs):
        """See documentation in base class."""
        # TODO:
        # - insert rows in observation table
        pass
