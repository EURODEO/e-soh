from storagebackend import StorageBackend
import common
from pgopbackend import Psycopg2BE, PsqlBE
import json
import sys

# NOTE: we assume that the risk of SQL injection is zero in this context


class PostGISSBE(StorageBackend):
    """A storage backend that uses a PostGIS database for storage of observations,
    per observation metadata, and per time series metadata."""

    def __init__(self, verbose, pg_conn_info):
        super().__init__(verbose, 'PostGIS')

        self._conn_info = pg_conn_info

        # recreate database from scratch
        self.__drop_database()
        self.__create_database()

        # create a database operation executor backend
        if common.get_env_var('PGOPBACKEND', 'psycopg2') == 'psycopg2':
            self._pgopbe = Psycopg2BE(verbose, self._conn_info)
        else:
            self._pgopbe = PsqlBE(verbose, self._conn_info)

        # install the postgis extension
        self.__install_postgis_extension()

    def __drop_database(self):
        """Drop any existing database named self._conn_info.dbname()."""

        if self._verbose:
            print(
                'dropping database {} ... '.format(self._conn_info.dbname()), file=sys.stderr,
                end='', flush=True)
        common.exec_command([
            'dropdb', '-w', '-f', '--if-exists', '-h', self._conn_info.host(),
            '-p', self._conn_info.port(), '-U', self._conn_info.user(), self._conn_info.dbname()])
        if self._verbose:
            print('done', file=sys.stderr, flush=True)

    def __create_database(self):
        """Create database named self._conn_info.dbname()."""

        if self._verbose:
            print(
                'creating database {} ... '.format(self._conn_info.dbname()), file=sys.stderr,
                end='', flush=True)
        common.exec_command([
            'createdb', '-w', '-h', self._conn_info.host(), '-p', self._conn_info.port(),
            '-U', self._conn_info.user(), self._conn_info.dbname()])
        if self._verbose:
            print('done', file=sys.stderr, flush=True)

    def __install_postgis_extension(self):
        """Install the PostGIS extension."""

        if self._verbose:
            print('installing PostGIS extension ... ', file=sys.stderr, end='', flush=True)
        self._pgopbe.execute('CREATE EXTENSION postgis')
        if self._verbose:
            print('done', file=sys.stderr, flush=True)

    def reset(self, tss):
        """See documentation in base class."""

        if self._verbose:
            print('resetting PostGISSBE with {} time series'.format(len(tss)), file=sys.stderr)

        # assume at this point that self._conn_info.dbname() exists, but not that it is
        # empty, so first step is to drop schema (all tables and indexes):
        self._pgopbe.execute('DROP TABLE IF EXISTS ts')
        # self._pgopbe.execute('DROP INDEX IF EXISTS ...')  # TODO?

        # create time series table
        self._pgopbe.execute(
            '''
                CREATE TABLE time_series (
                    id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    station_id text NOT NULL,
                    param_id text NOT NULL,
                    UNIQUE (station_id, param_id),
                    lat double precision NOT NULL,
                    lon double precision NOT NULL,
                    other_metadata jsonb NOT NULL
                )
            ''')

        # insert rows in time series table
        for ts in tss:

            cmd = '''
                INSERT INTO time_series (station_id, param_id, lat, lon, other_metadata)
                VALUES ('{}', '{}', {}, {}, '{}'::jsonb)
            '''
            self._pgopbe.execute(
                ' '.join(cmd.split()).format(
                    ts.station_id(), ts.param_id(), ts.lat(), ts.lon(),
                    json.dumps(ts.other_mdata())
                )
            )

        # - create indexes
        # TODO

        # ensure that PostGIS is enabled to perform quick geo searches for this case
        # (tss inside circle and polygon)
        # TODO

        # create observations table
        self._pgopbe.execute(
            '''
                CREATE TABLE observations (
                    ts_id integer REFERENCES time_series(id) ON DELETE CASCADE,
                    tstamp timestamp, -- obs time (NOT NULL, but implied by being part of PK)
                    value double precision, -- obs value
                    PRIMARY KEY (ts_id, tstamp)
                )
            ''')

    def set_obs(self, ts, times, obs):
        """See documentation in base class."""

        if self._verbose:
            print('setting observations in PostGIS SBE for time series >>>', file=sys.stderr)
            print('    ts: {}\n    times: ({} values), obs: ({} values)'.format(
                ts.__dict__, len(times), len(obs)), file=sys.stderr)

        # insert rows in observations table

        query = 'SELECT id FROM time_series WHERE station_id = \'{}\' AND param_id = \'{}\''
        rows = self._pgopbe.execute(query.format(ts.station_id(), ts.param_id()))
        ts_id = int(rows[0][0])  # assuming for now this always works (i.e. don't handle any error)

        values = []
        for to in zip(times, obs):
            values.append('({},to_timestamp({}),\'{}\')'.format(ts_id, to[0], to[1]))

        cmd = 'INSERT INTO observations (ts_id, tstamp, value) VALUES {};'.format(','.join(values))
        self._pgopbe.execute(cmd)

    def add_obs(self, ts, times, obs):
        """See documentation in base class."""
        # TODO

    def get_obs(self, tss, from_time, to_time):
        """See documentation in base class."""
        # TODO

    def get_obs_all(self, from_time, to_time):
        """See documentation in base class."""

        query = '''
            SELECT ts_id,tstamp,value FROM observations WHERE tstamp >= to_timestamp({})
            AND tstamp < to_timestamp({}) ORDER BY ts_id,tstamp
        '''
        return self._pgopbe.execute(query.format(from_time, to_time))

    def get_tss_in_circle(self, lat, lon, radius):
        """See documentation in base class."""
        # TODO
