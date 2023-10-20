import json
import sys

import common
from pgopbackend import PsqlBE
from pgopbackend import Psycopg2BE
from storagebackend import StorageBackend

# NOTE: we assume that the risk of SQL injection is zero in this context


class PostGISSBE(StorageBackend):
    """A storage backend that uses a PostGIS database for storage of observations,
    per observation metadata, and per time series metadata."""

    def __init__(self, verbose, pg_conn_info):
        super().__init__(verbose, "PostGIS")

        self._conn_info = pg_conn_info

        # recreate database from scratch
        self.__drop_database()
        self.__create_database()

        # create a database operation executor backend
        if common.get_env_var("PGOPBACKEND", "psycopg2") == "psycopg2":
            self._pgopbe = Psycopg2BE(verbose, self._conn_info)
        else:
            self._pgopbe = PsqlBE(verbose, self._conn_info)

        # install the postgis extension
        self.__install_postgis_extension()

    def pg_config(self):
        """Returns Postgres config."""

        return {
            "operation backend": self._pgopbe.descr(),
            "host": self._conn_info.host(),
            "port": self._conn_info.port(),
            "user": self._conn_info.user(),
            "password": "(not shown)",
            "dbname": self._conn_info.dbname(),
        }

    def __drop_database(self):
        """Drop any existing database named self._conn_info.dbname()."""

        if self._verbose:
            print(
                "dropping database {} ... ".format(self._conn_info.dbname()),
                file=sys.stderr,
                end="",
                flush=True,
            )
        common.exec_command(
            [
                "dropdb",
                "-w",
                "-f",
                "--if-exists",
                "-h",
                self._conn_info.host(),
                "-p",
                self._conn_info.port(),
                "-U",
                self._conn_info.user(),
                self._conn_info.dbname(),
            ]
        )
        if self._verbose:
            print("done", file=sys.stderr, flush=True)

    def __create_database(self):
        """Create database named self._conn_info.dbname()."""

        if self._verbose:
            print(
                "creating database {} ... ".format(self._conn_info.dbname()),
                file=sys.stderr,
                end="",
                flush=True,
            )
        common.exec_command(
            [
                "createdb",
                "-w",
                "-h",
                self._conn_info.host(),
                "-p",
                self._conn_info.port(),
                "-U",
                self._conn_info.user(),
                self._conn_info.dbname(),
            ]
        )
        if self._verbose:
            print("done", file=sys.stderr, flush=True)

    def __install_postgis_extension(self):
        """Install the PostGIS extension."""

        if self._verbose:
            print("installing PostGIS extension ... ", file=sys.stderr, end="", flush=True)
        self._pgopbe.execute("CREATE EXTENSION postgis")
        if self._verbose:
            print("done", file=sys.stderr, flush=True)

    def __get_ts_id(self, station_id, param_id):
        """Get time series ID from station_id and param_id"""

        query = "SELECT id FROM time_series WHERE station_id = '{}' AND param_id = '{}'"
        rows = self._pgopbe.execute(query.format(station_id, param_id))
        return int(rows[0][0])  # assuming for now this always works (i.e. don't handle any error)

    def __create_insert_values(self, ts_id, times, values):
        """Creates a list of strings to be used for VALUES in the INSERT command."""

        ivalues = []
        for to in zip(times, values):
            ivalues.append("({},to_timestamp({}),'{}')".format(ts_id, to[0], to[1]))
        return ivalues

    def reset(self, tss):
        """See documentation in base class."""

        if self._verbose:
            print("resetting PostGISSBE with {} time series".format(len(tss)), file=sys.stderr)

        # assume at this point that self._conn_info.dbname() exists, but not that it is
        # empty, so first step is to drop schema (all tables and indexes):
        self._pgopbe.execute("DROP TABLE IF EXISTS ts")
        # self._pgopbe.execute('DROP INDEX IF EXISTS ...')  # TODO?

        # create time series table
        self._pgopbe.execute(
            """
                CREATE TABLE time_series (
                    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    station_id TEXT NOT NULL,
                    param_id TEXT NOT NULL,
                    UNIQUE (station_id, param_id),
                    pos GEOGRAPHY(Point) NOT NULL,
                    other_metadata JSONB NOT NULL
                )
            """
        )

        # insert rows in time series table
        for ts in tss:
            cmd = """
                INSERT INTO time_series (station_id, param_id, pos, other_metadata)
                VALUES ('{}', '{}', '{}', '{}'::jsonb)
            """
            self._pgopbe.execute(
                " ".join(cmd.split()).format(
                    ts.station_id(),
                    ts.param_id(),
                    "POINT({} {})".format(ts.lon(), ts.lat()),
                    json.dumps(ts.other_mdata()),
                )
            )

        # - create indexes?
        # TODO

        # create observations table
        self._pgopbe.execute(
            """
                CREATE TABLE observations (
                    ts_id integer REFERENCES time_series(id) ON DELETE CASCADE,
                    tstamp timestamp, -- obs time (NOT NULL, but implied by being part of PK)
                    value double precision, -- obs value
                    PRIMARY KEY (ts_id, tstamp)
                )
            """
        )

    def set_obs(self, ts, times, values):
        """See documentation in base class."""

        if self._verbose:
            print("setting observations in PostGIS SBE for time series >>>", file=sys.stderr)
            print(
                "    ts: {}\n    times: (size: {}), values: (size: {})".format(
                    ts.__dict__, len(times), len(values)
                ),
                file=sys.stderr,
            )

        ts_id = self.__get_ts_id(ts.station_id(), ts.param_id())

        # replace all rows in observations table for this time series

        cmd = "DELETE FROM observations WHERE ts_id = {}".format(ts_id)
        self._pgopbe.execute(cmd)

        ivalues = self.__create_insert_values(ts_id, times, values)
        cmd = "INSERT INTO observations (ts_id, tstamp, value) VALUES {}".format(",".join(ivalues))
        self._pgopbe.execute(cmd)

    def add_obs(self, ts, times, values, oldest_time=None):
        """See documentation in base class."""

        ts_id = self.__get_ts_id(ts.station_id(), ts.param_id())

        # insert or update (i.e. "upsert") rows in observations table for this time series

        ivalues = self.__create_insert_values(ts_id, times, values)
        cmd = """
            INSERT INTO observations (ts_id, tstamp, value) VALUES {}
            ON CONFLICT ON CONSTRAINT observations_pkey DO UPDATE SET value = EXCLUDED.value
        """.format(
            ",".join(ivalues)
        )
        self._pgopbe.execute(cmd)

        if oldest_time is not None:  # delete observations that are too old
            cmd = """
                DELETE FROM observations WHERE ts_id = {} AND EXTRACT(EPOCH FROM tstamp) < {}
            """.format(
                ts_id, oldest_time
            )
            self._pgopbe.execute(cmd)

    def get_obs(self, ts_ids, from_time, to_time):
        """See documentation in base class."""
        # TODO

    def get_obs_all(self, from_time, to_time):
        """See documentation in base class."""

        query = """
            SELECT ts_id,
                array_agg(CAST(EXTRACT(EPOCH FROM tstamp) AS int) ORDER BY tstamp),
                array_agg(value ORDER BY tstamp)
            FROM observations WHERE tstamp >= to_timestamp({}) AND tstamp < to_timestamp({})
            GROUP BY ts_id
        """

        rows = self._pgopbe.execute(query.format(from_time, to_time))

        res = []
        for row in rows:
            ts_id, times, values = int(row[0]), row[1], row[2]
            if isinstance(times, str):  # the case for PsqlBE
                # convert '{ITEM1, ITEM2, ..., ITEMN}' to
                # [convert(ITEM1), convert(ITEM2), ..., convert(ITEMN)]
                times = [int(x) for x in times.strip()[1:-1].split(",")]
                values = [float(x) for x in values.strip()[1:-1].split(",")]
            # assert(isinstance(times, list))
            # assert(isinstance(values, list))
            res.append((ts_id, times, values))

        return res

    def get_station_and_param(self, ts_id):
        """See documentation in base class."""

        query = "SELECT station_id, param_id FROM time_series WHERE id = {}"
        rows = self._pgopbe.execute(query.format(ts_id))
        return rows[0][0], rows[0][1]

    def get_ts_ids_all(self):
        """See documentation in base class."""

        rows = self._pgopbe.execute("SELECT id FROM time_series")
        return [int(row[0]) for row in rows]

    def get_ts_ids_in_circle(self, lat, lon, radius):
        """See documentation in base class."""

        query = """
            SELECT id FROM time_series
            WHERE ST_Distance('SRID=4326;POINT({} {})'::geography, pos) < {}
        """
        rows = self._pgopbe.execute(query.format(lon, lat, radius))
        return [int(row[0]) for row in rows]
