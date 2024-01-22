import sys
from abc import ABC
from abc import abstractmethod

import common
import psycopg2


class PGOpBackend(ABC):
    """The base class / interface for an executor backend for a Postgres database operation
    (query or command).
    """

    def __init__(self, verbose, descr):
        self._verbose = verbose
        self._descr = descr

    def descr(self):
        return self._descr

    @abstractmethod
    def execute(self, op, commit):
        """Execute database operation.

        Returns a list of row tuples with string values:
            [(val1, val2, ...), (val1, val2, ...), ...]
        """

    @abstractmethod
    def commit(self):
        """Commits last operation."""


class Psycopg2BE(PGOpBackend):
    """A Postgres backend that uses the psycopg2 adapter."""

    def __init__(self, verbose, conn_info):
        if verbose:
            print("using psycopg2 adapter for PostGIS operations", file=sys.stderr)
        super().__init__(verbose, "psycopg2 adapter")
        self._conn = self.__connect(conn_info)
        self._cur = self._conn.cursor()

    def __connect(self, conn_info):
        """ "Connect to the database server.
        Returns connection.
        """

        if self._verbose:
            start = common.now_secs()
            print("connecting to PostGIS ... ", file=sys.stderr, end="", flush=True)

        # WARNING: the call to connect() may take very long; up to 15-20 secs!
        conn = psycopg2.connect(
            "host={} port={} user={} password={} dbname={}".format(
                conn_info.host(),
                conn_info.port(),
                conn_info.user(),
                conn_info.password(),
                conn_info.dbname(),
            )
        )
        if self._verbose:
            print(
                "done (after {0:.4f} secs)".format(common.now_secs() - start),
                file=sys.stderr,
                flush=True,
            )
        return conn

    def execute(self, op, commit=True):
        """See documentation in base class."""

        self._cur.execute(op)
        if commit:
            self._conn.commit()

        try:
            return self._cur.fetchall()
        except Exception:
            return []  # nothing to fetch

    def commit(self):
        """See documentation in base class."""

        self._conn.commit()


class PsqlBE(PGOpBackend):
    """A backend that uses the psql command."""

    def __init__(self, verbose, conn_info):
        if verbose:
            print("using psql command for PostGIS operations", file=sys.stderr)
        super().__init__(verbose, "psql command")
        self._conn_info = conn_info

    def execute(self, op, commit=False):
        """See documentation in base class."""

        _ = commit  # n/a
        res = common.exec_command(
            [
                "psql",
                "-t",
                "-h",
                self._conn_info.host(),
                "-p",
                self._conn_info.port(),
                "-U",
                self._conn_info.user(),
                "-d",
                self._conn_info.dbname(),
                "-c",
                op,
            ]
        )

        res = [x for x in res.decode("utf-8").split("\n") if len(x) > 0]
        return list(map(lambda x: tuple([z.strip() for z in x.split("|")]), res))

    def commit(self):
        """See documentation in base class."""

        # no-op, since n/a
