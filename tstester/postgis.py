from storagebackend import StorageBackend
from abc import ABC, abstractmethod
import common
import psycopg2


class DbConnectionInfo:
    """Keeps database connection info."""

    def __init__(self, verbose, host, port, user, password, dbname):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        if dbname.strip().lower() == 'postgres':
            raise Exception('database name must be different from \'postgres\'')
        self._dbname = dbname

    def host(self):
        return self._host

    def port(self):
        return self._port

    def user(self):
        return self._user

    def password(self):
        return self._password

    def dbname(self):
        return self._dbname


class DbOpBackend(ABC):
    """The base class / interface for an executor backend for a database operation
       (query or command).
    """

    def __init__(self, verbose):
        self._verbose = verbose

    @abstractmethod
    def execute(self, op):
        """Execute database operation.
        Return result as string.
        """


class Psycopg2Executor(DbOpBackend):
    """A backend that uses the psycopg2 adapter."""

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


class PsqlExecutor(DbOpBackend):
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


class PostGIS(StorageBackend):
    """A storage backend that uses a PostGIS database for storage."""

    def __init__(self, verbose, host, port, user, password, dbname):
        super().__init__(verbose, 'PostGIS')

        self._conn_info = DbConnectionInfo(verbose, host, port, user, password, dbname)

        # recreate database from scratch
        self.__drop_database()
        self.__create_database()

        # create a database operation executor backend
        if common.get_env_var('PGOPBACKEND', 'psycopg2') == 'psycopg2':
            self._dbop = Psycopg2Executor(verbose, self._conn_info)
        else:
            self._dbop = PsqlExecutor(verbose, self._conn_info)

        # install the postgis extension
        self.__install_postgis_extension()

    def __drop_database(self):
        """Drop any existing database named self._conn_info.dbname()."""

        if self._verbose:
            print('dropping database {} ... '.format(self._conn_info.dbname()), end='', flush=True)
        common.exec_command([
            'dropdb', '-w', '-f', '--if-exists', '-h', self._conn_info.host(),
            '-p', self._conn_info.port(), '-U', self._conn_info.user(), self._conn_info.dbname()])
        if self._verbose:
            print('done', flush=True)

    def __create_database(self):
        """Create database named self._conn_info.dbname()."""

        if self._verbose:
            print('creating database {} ... '.format(self._conn_info.dbname()), end='', flush=True)
        common.exec_command([
            'createdb', '-w', '-h', self._conn_info.host(), '-p', self._conn_info.port(),
            '-U', self._conn_info.user(), self._conn_info.dbname()])
        if self._verbose:
            print('done', flush=True)

    def __install_postgis_extension(self):
        """Install the PostGIS extension."""

        if self._verbose:
            print('installing PostGIS extension ... ', end='', flush=True)
        self._dbop.execute('CREATE EXTENSION postgis')
        if self._verbose:
            print('done', flush=True)

    def reset(self, tss):
        """See documentation in base class."""

        if self._verbose:
            print('\nresetting PostGIS SBE with {} time series ... TODO'.format(len(tss)))

        # assume at this point that self._conn_info.dbname() is empty

        # TODO:
        # - create schema (including a time series table and an observation table) based on info
        #   in tss
        # - insert rows in time series table

    def set_observations(self, ts, times, obs):

        """See documentation in base class."""

        # TODO:
        # - insert rows in observation table
        pass
