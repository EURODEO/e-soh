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

    def pg_config(self):
        """Returns Postgres config."""

        return {
            'operation backend': self._pgopbe.descr(),
            'host': self._conn_info.host(),
            'port': self._conn_info.port(),
            'user': self._conn_info.user(),
            'password': '(not shown)',
            'dbname': self._conn_info.dbname()
        }

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
                    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    station_id TEXT NOT NULL,
                    param_id TEXT NOT NULL,
                    UNIQUE (station_id, param_id),
                    pos GEOGRAPHY(Point) NOT NULL,
                    other_metadata JSONB NOT NULL
                )
            ''')

        # insert rows in time series table
        for ts in tss:

            cmd = '''
                INSERT INTO time_series (station_id, param_id, pos, other_metadata)
                VALUES ('{}', '{}', '{}', '{}'::jsonb)
            '''
            self._pgopbe.execute(
                ' '.join(cmd.split()).format(
                    ts.station_id(), ts.param_id(),
                    'POINT({} {})'.format(ts.lon(), ts.lat()),
                    json.dumps(ts.other_mdata())
                )
            )

        # - create indexes?
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

    def add_obs(self, ts, times, obs, oldest_time=None):
        """See documentation in base class."""
        # TODO

    def get_obs(self, ts_ids, from_time, to_time):
        """See documentation in base class."""
        # TODO

    def get_obs_all(self, from_time, to_time):
        """See documentation in base class."""

        query = '''
            SELECT ts_id,
                array_agg(CAST(EXTRACT(EPOCH FROM tstamp) AS int) ORDER BY tstamp),
                array_agg(value ORDER BY tstamp)
            FROM observations WHERE tstamp >= to_timestamp({}) AND tstamp < to_timestamp({})
            GROUP BY ts_id
        '''

        rows = self._pgopbe.execute(query.format(from_time, to_time))

        res = []
        for row in rows:
            ts_id, times, obs = int(row[0]), row[1], row[2]
            if isinstance(times, str):  # the case for PsqlBE
                # convert '{ITEM1, ITEM2, ..., ITEMN}' to
                # [convert(ITEM1), convert(ITEM2), ..., convert(ITEMN)]
                times = [int(x) for x in times.strip()[1:-1].split(',')]
                obs = [float(x) for x in obs.strip()[1:-1].split(',')]
            # assert(isinstance(times, list))
            # assert(isinstance(obs, list))
            res.append((ts_id, times, obs))

        return res

    def get_station_and_param(self, ts_id):
        """See documentation in base class."""

        query = 'SELECT station_id, param_id FROM time_series WHERE id = {}'
        rows = self._pgopbe.execute(query.format(ts_id))
        return rows[0][0], rows[0][1]

    def get_ts_ids_all(self):
        """See documentation in base class."""

        rows = self._pgopbe.execute('SELECT id FROM time_series')
        return [int(row[0]) for row in rows]

    def get_ts_ids_in_circle(self, lat, lon, radius):
        """See documentation in base class."""

        query = '''
            SELECT id FROM time_series
            WHERE ST_Distance('SRID=4326;POINT({} {})'::geography, pos) < {}
        '''
        rows = self._pgopbe.execute(query.format(lon, lat, radius))
        return [int(row[0]) for row in rows]
