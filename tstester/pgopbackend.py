from abc import ABC, abstractmethod
import common
import psycopg2


class PGOpBackend(ABC):
    """The base class / interface for an executor backend for a Postgres database operation
       (query or command).
    """

    def __init__(self, verbose):
        self._verbose = verbose

    @abstractmethod
    def execute(self, op):
        """Execute database operation.
        Return result as string.
        """


class Psycopg2BE(PGOpBackend):
    """A Postgres backend that uses the psycopg2 adapter."""

    def __init__(self, verbose, conn_info):
        if verbose:
            print('using psycopg2 adapter for PostGIS operations')
        super().__init__(verbose)
        self._conn = self.__connect(conn_info)
        self._cur = self._conn.cursor()

    def __connect(self, conn_info):
        """"Connect to the database server.
        Returns connection.
        """

        if self._verbose:
            start = common.now_secs()
            print('connecting to PostGIS ... ', end='', flush=True)

        # WARNING: the call to connect() may take very long; up to 15-20 secs!
        conn = psycopg2.connect('host={} port={} user={} password={} dbname={}'.format(
            conn_info.host(), conn_info.port(), conn_info.user(), conn_info.password(),
            conn_info.dbname()
        ))
        if self._verbose:
            print('done (after {} secs)'.format(common.now_secs() - start), flush=True)
        return conn

    def execute(self, op):
        """See documentation in base class."""

        res = self._cur.execute(op)
        self._conn.commit()
        return res


class PsqlBE(PGOpBackend):
    """A backend that uses the psql command."""

    def __init__(self, verbose, conn_info):
        if verbose:
            print('using psql command for PostGIS operations')
        super().__init__(verbose)
        self._conn_info = conn_info

    def execute(self, op):
        """See documentation in base class."""

        res = common.exec_command([
            'psql', '-h', self._conn_info.host(), '-p', self._conn_info.port(),
            '-U', self._conn_info.user(), '-d', self._conn_info.dbname(), '-c', op])
        return res
