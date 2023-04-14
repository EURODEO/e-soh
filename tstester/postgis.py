from storagebackend import StorageBackend
import psycopg2
import common


class PostGIS(StorageBackend):
    """A storage backend that uses a PostGIS instance for storage."""

    def __init__(self, verbose, host, port, user, password, dbname):
        super().__init__(verbose, 'PostGIS')
        if verbose:
            start = common.now_secs()
            print('connecting to PostGIS ... ', end='', flush=True)
        self._conn = psycopg2.connect('host={} port={} user={} password={} dbname={}'.format(
            host, port, user, password, dbname
        ))
        if verbose:
            print('done (after {} secs)'.format(common.now_secs() - start), flush=True)

        self._cur = self._conn.cursor()

    def reset(self, tss):
        """See documentation in base class."""
        if self._verbose:
            print('\nresetting PostGIS SBE with {} time series ... TODO'.format(len(tss)))
        # TODO:
        # - drop database
        # - create schema based on info in tss
        # - insert rows in time series table

    def set_observations(self, ts, times, obs):
        """See documentation in base class."""
        # TODO:
        # - insert rows in observation table
        pass
